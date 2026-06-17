#include <switch.h>
#include <switch_json.h>
#include <string.h>
#include <strings.h>
#include <string>
#include <mutex>
#include <thread>
#include <list>
#include <deque>
#include <unordered_map>
#include <memory>
#include <algorithm>
#include <functional>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <regex>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "base64.hpp"
#include "parser.hpp"
#include "mod_audio_docker.h"
#include "audio_pipe.hpp"

// #define RTP_PACKETIZATION_PERIOD 20
// #define FRAME_SIZE_8000  320 /*which means each 20ms frame as 320 bytes at 8 khz (1 channel only)*/
// #define AUDIO_DOCKER_FRAME_SIZE  FRAME_SIZE_8000  * 15 /*which means each 150ms*/

namespace {
  static const char *requestedBufferSecs = std::getenv("MOD_AUDIO_DOCKER_BUFFER_SECS");
  static const char *numberOfFramesForStreaming = std::getenv("MOD_AUDIO_DOCKER_FRAME_SIZE");
  static int nAudioBufferSecs = std::max(1, std::min(requestedBufferSecs ? ::atoi(requestedBufferSecs) : 2, 5));
  static const char *requestedNumServiceThreads = std::getenv("MOD_AUDIO_DOCKER_THREADS");
  static const char *playAudioDirection = std::getenv("MOD_AUDIO_DOCKER_PLAY_AUDIO_DIRECTION") ? std::getenv("MOD_AUDIO_DOCKER_PLAY_AUDIO_DIRECTION") : PLAY_AUDIO_TO_A_LEG; // PLAY_AUDIO_TO_A_LEG, PLAY_AUDIO_TO_B_LEG or PLAY_AUDIO_TO_BOTH
  static const char *displaceAudio = std::getenv("MOD_AUDIO_DOCKER_DISPLACE_AUDIO") ? std::getenv("MOD_AUDIO_DOCKER_DISPLACE_AUDIO") : DISPLACE_AUDIO; // YES or NO
  static const char *freeswitchHome = std::getenv("HOME") ? std::getenv("HOME") : "/usr/local/freeswitch";
  static const char *audioDockerServer = std::getenv("MOD_AUDIO_DOCKER_SERVER") ? std::getenv("MOD_AUDIO_DOCKER_SERVER") : "localhost:8080"; 
  static const char* mySubProtocolName = std::getenv("MOD_AUDIO_DOCKER_SUBPROTOCOL_NAME") ?
    std::getenv("MOD_AUDIO_DOCKER_SUBPROTOCOL_NAME") : "streaming";
  static unsigned int nServiceThreads = std::max(1, std::min(requestedNumServiceThreads ? ::atoi(requestedNumServiceThreads) : 1, 5));
  static unsigned int idxCallCount = 0;
	  static uint32_t skip_printing = 0;
	  static size_t streaming_size = FRAME_SIZE_8000 * ::atoi(numberOfFramesForStreaming);
	  static std::mutex playbackSequenceMutex;
	  static std::unordered_map<std::string, uint64_t> playbackSequences;


	void parse_wav_header(unsigned char *header) {
	    if (strncmp((const char *)header, "RIFF", 4)) {
	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - This is not a valid WAV file.\n");
	      return;
	    }

	    // Assuming header is the first 44 bytes of the WAV file
	    int audioFormat = header[20] | (header[21] << 8);  // Format code
	    int numChannels = header[22] | (header[23] << 8);  // Number of channels
	    int sampleRate = header[24] | (header[25] << 8) | (header[26] << 16) | (header[27] << 24);  // Sample rate

	    // Check audio format - PCM should be 1
	    if (audioFormat != 1) {
	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "This is not PCM format.\n");
	        return;
	    }
	    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "Sample Rate: %d, Number of Channels:%d\n", sampleRate, numChannels);

	    // You can extract more information as needed
	}

	uint32_t read_le_u32(const unsigned char *data) {
	    return ((uint32_t)data[0]) | ((uint32_t)data[1] << 8) | ((uint32_t)data[2] << 16) | ((uint32_t)data[3] << 24);
	}

	uint16_t read_le_u16(const unsigned char *data) {
	    return ((uint16_t)data[0]) | ((uint16_t)data[1] << 8);
	}

	uint32_t wav_duration_ms(const char *message, size_t length) {
	    if (!message || length < 44 || strncmp(message, "RIFF", 4) || strncmp(message + 8, "WAVE", 4)) {
	      return 0;
	    }

	    const unsigned char *data = reinterpret_cast<const unsigned char *>(message);
	    uint16_t channels = read_le_u16(data + 22);
	    uint32_t sample_rate = read_le_u32(data + 24);
	    uint16_t bits_per_sample = read_le_u16(data + 34);
	    uint32_t data_size = read_le_u32(data + 40);

	    if (!channels || !sample_rate || !bits_per_sample || !data_size) {
	      return 0;
	    }

	    uint32_t bytes_per_second = sample_rate * channels * (bits_per_sample / 8);
	    if (!bytes_per_second) {
	      return 0;
	    }

	    return (uint32_t)(((uint64_t)data_size * 1000) / bytes_per_second);
	}

	bool ensure_directory(const std::string& path) {
	    if (path.empty()) {
	      return false;
	    }

	    std::string current;
	    size_t pos = 0;
	    if (path[0] == '/') {
	      current = "/";
	      pos = 1;
	    }

	    while (pos <= path.size()) {
	      size_t next = path.find('/', pos);
	      std::string part = path.substr(pos, next == std::string::npos ? std::string::npos : next - pos);
	      if (!part.empty()) {
	        if (current.size() > 1 && current.back() != '/') {
	          current += "/";
	        }
	        current += part;
	        if (mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) {
	          switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "audio_docker playback queue - failed to create directory %s: %s\n", current.c_str(), strerror(errno));
	          return false;
	        }
	      }
	      if (next == std::string::npos) {
	        break;
	      }
	      pos = next + 1;
	    }

	    return true;
	}

	uint64_t next_playback_sequence(const char *sessionId) {
	    std::lock_guard<std::mutex> lock(playbackSequenceMutex);
	    return ++playbackSequences[sessionId ? sessionId : ""];
	}

	void clear_playback_sequence(const char *sessionId) {
	    if (!sessionId || !*sessionId) {
	      return;
	    }

	    std::lock_guard<std::mutex> lock(playbackSequenceMutex);
	    playbackSequences.erase(sessionId);
	}

	std::string write_unique_wav_file(const char *sessionId, const char *message, size_t length) {
	    uint64_t seq = next_playback_sequence(sessionId);
	    std::string dir = std::string(freeswitchHome) + "/audio_docker/" + sessionId;
	    if (!ensure_directory(dir)) {
	      return "";
	    }

	    std::string path = dir + "/" + sessionId + "-" + std::to_string(seq) + ".wav";
	    FILE* file = fopen(path.c_str(), "wb");
	    if (!file) {
	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "processIncomingMessage - failed to open audio file for write: %s\n", path.c_str());
	      return "";
	    }

	    size_t written = fwrite(message, sizeof(char), length, file);
	    fclose(file);
	    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - store audio into file: %s message-len:%d\n", path.c_str(), length);

	    if (written != length) {
	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "processIncomingMessage - Failed to write all audio data - written: %d, len: %d\n", written, length);
	      std::remove(path.c_str());
	      return "";
	    }

	    return path;
	}

	class PlaybackQueueManager {
	public:
	    static PlaybackQueueManager& instance() {
	      static PlaybackQueueManager manager;
	      return manager;
	    }

	    void enqueue(const std::string& target_uuid, const std::string& file, bool displace, uint32_t duration_ms) {
	      if (target_uuid.empty() || file.empty()) {
	        return;
	      }

	      std::shared_ptr<TargetQueue> queue;
	      {
	        std::lock_guard<std::mutex> lock(m_queues_mutex);
	        queue = m_queues[target_uuid];
	        if (!queue) {
	          queue = std::make_shared<TargetQueue>();
	          m_queues[target_uuid] = queue;
	        }
	        queue->files.push_back(PlaybackItem{file, displace, duration_ms, (uint64_t)switch_micro_time_now()});
	        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - queued target:%s depth:%lu file:%s\n", target_uuid.c_str(), (unsigned long)queue->files.size(), file.c_str());
	        if (!queue->running) {
	          queue->running = true;
	          std::thread(&PlaybackQueueManager::worker, this, target_uuid).detach();
	        }
	      }
	    }

	    size_t clear(const std::string& target_uuid) {
	      size_t removed = 0;

	      if (target_uuid.empty()) {
	        return removed;
	      }

	      std::lock_guard<std::mutex> lock(m_queues_mutex);
	      auto it = m_queues.find(target_uuid);
	      if (it == m_queues.end()) {
	        return removed;
	      }

	      while (!it->second->files.empty()) {
	        PlaybackItem item = it->second->files.front();
	        it->second->files.pop_front();
	        if (std::remove(item.file.c_str()) == 0) {
	          switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - removed queued file target:%s file:%s\n", target_uuid.c_str(), item.file.c_str());
	        } else {
	          switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - failed to remove queued file target:%s file:%s\n", target_uuid.c_str(), item.file.c_str());
	        }
	        removed++;
	      }

	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - clear target:%s removed:%lu\n", target_uuid.c_str(), (unsigned long)removed);
	      return removed;
	    }

	private:
	    struct PlaybackItem {
	      std::string file;
	      bool displace;
	      uint32_t duration_ms;
	      uint64_t enqueue_time_us;
	    };

	    struct TargetQueue {
	      std::deque<PlaybackItem> files;
	      bool running = false;
	    };

	    std::mutex m_queues_mutex;
	    std::unordered_map<std::string, std::shared_ptr<TargetQueue>> m_queues;

	    void worker(std::string target_uuid) {
	      for (;;) {
	        PlaybackItem item;
	        {
	          std::lock_guard<std::mutex> lock(m_queues_mutex);
	          auto it = m_queues.find(target_uuid);
	          if (it == m_queues.end() || it->second->files.empty()) {
	            if (it != m_queues.end()) {
	              it->second->running = false;
	              m_queues.erase(it);
	            }
	            break;
	          }
	          item = it->second->files.front();
	          it->second->files.pop_front();
	        }

	        play_item(target_uuid, item);
	      }

	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - worker idle for target:%s\n", target_uuid.c_str());
	    }

	    void play_item(const std::string& target_uuid, const PlaybackItem& item) {
	      switch_core_session_t *target_session = switch_core_session_locate(target_uuid.c_str());
	      if (!target_session) {
	        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "audio_docker playback queue - target session missing: %s, deleting %s\n", target_uuid.c_str(), item.file.c_str());
	        std::remove(item.file.c_str());
	        return;
	      }

	      switch_status_t status = SWITCH_STATUS_FALSE;
	      uint64_t now_us = (uint64_t)switch_micro_time_now();
	      uint64_t wait_ms = now_us >= item.enqueue_time_us ? (now_us - item.enqueue_time_us) / 1000 : 0;
	      switch_channel_t *channel = switch_core_session_get_channel(target_session);
	      if (channel && switch_channel_ready(channel)) {
	        if (item.displace) {
	          switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - displace target:%s file:%s duration_ms:%u queue_wait_ms:%lu\n", target_uuid.c_str(), item.file.c_str(), item.duration_ms, (unsigned long)wait_ms);
	          status = switch_ivr_displace_session(target_session, item.file.c_str(), 0, NULL);
	          if (status == SWITCH_STATUS_SUCCESS) {
	            uint32_t remaining_ms = item.duration_ms + 100;
	            while (remaining_ms > 0 && switch_channel_ready(channel)) {
	              uint32_t sleep_ms = std::min<uint32_t>(remaining_ms, 100);
	              switch_yield((switch_interval_time_t)sleep_ms * 1000);
	              remaining_ms -= sleep_ms;
	            }
	            switch_ivr_stop_displace_session(target_session, item.file.c_str());
	          }
	        } else {
	          switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - play_file target:%s file:%s queue_wait_ms:%lu\n", target_uuid.c_str(), item.file.c_str(), (unsigned long)wait_ms);
	          status = switch_ivr_play_file(target_session, NULL, item.file.c_str(), NULL);
	        }
	      }

	      switch_core_session_rwunlock(target_session);

	      if (status != SWITCH_STATUS_SUCCESS) {
	        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - Failed to play audio file: %s\n", item.file.c_str());
	      } else {
	        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - Played audio file: %s\n", item.file.c_str());
	      }

	      if (std::remove(item.file.c_str()) == 0) {
	        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - The file %s was deleted successfully.\n", item.file.c_str());
	      } else {
	        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "audio_docker playback queue - Error deleting the file: %s\n", item.file.c_str());
	      }
	    }
	};

	void enqueue_playback_for_uuid(const char *target_uuid, const std::string& path, bool displace, uint32_t duration_ms) {
	    if (!target_uuid || !*target_uuid) {
	      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "audio_docker playback queue - empty target uuid for file: %s\n", path.c_str());
	      return;
	    }

	    PlaybackQueueManager::instance().enqueue(target_uuid, path, displace, duration_ms);
	}

	std::string get_metadata_value(const char *metadata, const char *key) {
    if (!metadata || !key || !*key) {
      return "";
    }

    std::string raw(metadata);
    std::string prefix = std::string(key) + "=";
    size_t start = 0;

    while (start < raw.size()) {
      size_t end = raw.find('/', start);
      std::string token = raw.substr(start, end == std::string::npos ? std::string::npos : end - start);
      if (token.rfind(prefix, 0) == 0) {
        return token.substr(prefix.size());
      }
      if (end == std::string::npos) {
        break;
      }
      start = end + 1;
    }

    return "";
}

const char *resolve_play_audio_direction(const char *metadata) {
    std::string playbackTarget = get_metadata_value(metadata, "X-Playback-Target");

    if (playbackTarget == "self") {
      return PLAY_AUDIO_TO_A_LEG;
    }
    if (playbackTarget == "other") {
      return PLAY_AUDIO_TO_B_LEG;
    }
    if (playbackTarget == "both") {
      return PLAY_AUDIO_TO_BOTH;
    }

    return playAudioDirection;
}

const char *resolve_control_target_direction(const char *metadata, const char *target) {
    if (target && *target) {
      if (!strcasecmp(target, "self")) {
        return PLAY_AUDIO_TO_A_LEG;
      }
      if (!strcasecmp(target, "other")) {
        return PLAY_AUDIO_TO_B_LEG;
      }
      if (!strcasecmp(target, "both")) {
        return PLAY_AUDIO_TO_BOTH;
      }
      if (!strcasecmp(target, PLAY_AUDIO_TO_A_LEG)) {
        return PLAY_AUDIO_TO_A_LEG;
      }
      if (!strcasecmp(target, PLAY_AUDIO_TO_B_LEG)) {
        return PLAY_AUDIO_TO_B_LEG;
      }
      if (!strcasecmp(target, PLAY_AUDIO_TO_BOTH)) {
        return PLAY_AUDIO_TO_BOTH;
      }

      return NULL;
    }

    return resolve_play_audio_direction(metadata);
}

size_t clear_playback_queue_for_uuid(const char *target_uuid) {
    if (!target_uuid || !*target_uuid) {
      return 0;
    }

    return PlaybackQueueManager::instance().clear(target_uuid);
}

size_t clear_playback_queue_for_direction(switch_core_session_t* session, const char *direction) {
    size_t removed = 0;
    const char* sessionId = switch_core_session_get_uuid(session);
    switch_channel_t *channel = switch_core_session_get_channel(session);
    const char *other_uuid = channel ? switch_channel_get_variable(channel, SWITCH_BRIDGE_UUID_VARIABLE) : NULL;

    if (strcmp(direction, PLAY_AUDIO_TO_A_LEG) == 0) {
      removed += clear_playback_queue_for_uuid(sessionId);
    } else if (strcmp(direction, PLAY_AUDIO_TO_B_LEG) == 0) {
      removed += clear_playback_queue_for_uuid(other_uuid);
    } else if (strcmp(direction, PLAY_AUDIO_TO_BOTH) == 0) {
      removed += clear_playback_queue_for_uuid(sessionId);
      removed += clear_playback_queue_for_uuid(other_uuid);
    }

    return removed;
}

bool handle_playback_control_message(private_t* tech_pvt, switch_core_session_t* session, const std::string& msg) {
    cJSON *json = cJSON_Parse(msg.c_str());
    if (!json) {
      return false;
    }

    bool handled = false;
    const char *type = cJSON_GetObjectCstr(json, "type");
    const char *action = cJSON_GetObjectCstr(json, "action");

    if (type && action && !strcasecmp(type, "playback_control") && !strcasecmp(action, "clear_queue")) {
      cJSON *finish_current = cJSON_GetObjectItem(json, "finish_current");
      if (finish_current && cJSON_IsBool(finish_current) && !cJSON_IsTrue(finish_current)) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "playback_control clear_queue with finish_current=false is unsupported; current prompt will not be interrupted\n");
        handled = true;
      } else {
        const char *target = cJSON_GetObjectCstr(json, "target");
        const char *direction = resolve_control_target_direction(tech_pvt->initialMetadata, target);

        if (direction) {
          size_t removed = clear_playback_queue_for_direction(session, direction);
          switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "playback_control clear_queue target:%s direction:%s removed:%lu\n", target ? target : "<default>", direction, (unsigned long)removed);
        } else {
          switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "playback_control clear_queue ignored unknown target:%s\n", target ? target : "<null>");
        }
        handled = true;
      }
    }

    cJSON_Delete(json);
    return handled;
}

  void processIncomingMessage(private_t* tech_pvt, switch_core_session_t* session, const char* msg_type, const char* message, size_t length) {
  std::string msg = message;
  std::string type  = msg_type;

    // switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "(%u) processIncomingMessage - received msg_type:%s, message:%s \n", tech_pvt->id, type, msg);
    if (!session) {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - unable to find session\n");
    } else {
	      if (type == "AUDIO") {
	          const char* sessionId = switch_core_session_get_uuid(session);
	          lwsl_notice("processIncomingMessage - (%s) AUDIO (len:%d)\n",sessionId, length);
	          if (session && switch_channel_ready(switch_core_session_get_channel(session))) {
	            if (length >= 44) {
	              unsigned char header[44] = {0};
	              memcpy(header, message, sizeof(header));
	              parse_wav_header(header);
	            } else {
	              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "processIncomingMessage - AUDIO message too short for WAV header, len:%d\n", length);
	            }
	            std::string path = write_unique_wav_file(sessionId, message, length);
	            if (path.empty()) {
	              return;
	            }

	            bool displace = strcmp(displaceAudio, DISPLACE_AUDIO) == 0;
	            uint32_t duration_ms = wav_duration_ms(message, length);
	            if (!duration_ms) {
	              duration_ms = 1000;
	            }

	            const char *effectivePlayAudioDirection = resolve_play_audio_direction(tech_pvt->initialMetadata);
	            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - playback target metadata:%s effective direction:%s\n", get_metadata_value(tech_pvt->initialMetadata, "X-Playback-Target").c_str(), effectivePlayAudioDirection);

	            if (strcmp(effectivePlayAudioDirection, PLAY_AUDIO_TO_A_LEG) == 0) {
	              enqueue_playback_for_uuid(sessionId, path, displace, duration_ms);
	            } else if (strcmp(effectivePlayAudioDirection, PLAY_AUDIO_TO_B_LEG) == 0) {
	              switch_channel_t *channel = switch_core_session_get_channel(session);
	              const char *other_uuid = switch_channel_get_variable(channel, SWITCH_BRIDGE_UUID_VARIABLE);
	              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - other_uuid: %s\n", other_uuid);
	              if (other_uuid && *other_uuid) {
	                enqueue_playback_for_uuid(other_uuid, path, displace, duration_ms);
	              } else {
	                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "processIncomingMessage (bridged_session) - Could not locate bridged uuid\n");
	                std::remove(path.c_str());
	              }
	            } else if (strcmp(effectivePlayAudioDirection, PLAY_AUDIO_TO_BOTH) == 0) {
	              switch_channel_t *channel = switch_core_session_get_channel(session);
	              const char *other_uuid = switch_channel_get_variable(channel, SWITCH_BRIDGE_UUID_VARIABLE);
	              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - other_uuid: %s\n", other_uuid);
	              enqueue_playback_for_uuid(sessionId, path, displace, duration_ms);
	              if (other_uuid && *other_uuid) {
	                std::string other_path = write_unique_wav_file(sessionId, message, length);
	                if (!other_path.empty()) {
	                  enqueue_playback_for_uuid(other_uuid, other_path, displace, duration_ms);
	                }
	              } else {
	                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING, "processIncomingMessage (PLAY_AUDIO_TO_BOTH) - Could not locate bridged uuid\n");
	              }
	            } else {
	              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "processIncomingMessage - EVENT_PLAY_AUDIO - path: %s\n",path.c_str());
	              tech_pvt->responseHandler(session, EVENT_PLAY_AUDIO, (char *) path.c_str());
	            }
          } else {
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_WARNING, "Cannot play audio. The channel is not ready or session is invalid.\n");
          }

        // switch_frame_t  write_frame = { 0 };
        // switch_status_t status;
        // // switch_channel_t *channel = switch_core_session_get_channel(session);
        // write_frame.data = (void*)message;
        // write_frame.datalen = strlen(message);
        // write_frame.codec = switch_core_session_get_read_codec(session);

        // status = switch_core_session_write_frame(session, &write_frame, SWITCH_IO_FLAG_NONE, 0);
        // if (status != SWITCH_STATUS_SUCCESS) {
        //   lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE failed to write frame to session - len: %d\n", strlen(msg.c_str()));
        // }
        // else {
        //   lwsl_notice("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE wrote frame to session: %d\n", strlen(msg.c_str()));
        // }
	      } else if (type == "MESSAGE"){
	        lwsl_notice("processIncomingMessage - MESSAGE (len:%d) message:%s\n",strlen(msg.c_str()), msg);
	        if (!handle_playback_control_message(tech_pvt, session, msg)) {
	          tech_pvt->responseHandler(session, EVENT_TEXT_MESSAGE, (char *) msg.c_str());
	        }
	      } else {
        lwsl_err("processIncomingMessage - unknown message type\n");
      }

    }


    // if (0 == type.compare("playAudio")) {

    //     // dont send actual audio bytes in event message
    //     cJSON* jsonFile = NULL;
    //     cJSON* jsonAudio = cJSON_DetachItemFromObject(jsonData, "audioContent");
    //     int validAudio = (jsonAudio && NULL != jsonAudio->valuestring);

    //     const char* szAudioContentType = cJSON_GetObjectCstr(jsonData, "audioContentType");
    //     char fileType[6];
    //     int sampleRate = 16000;
    //     if (0 == strcmp(szAudioContentType, "raw")) {
    //       cJSON* jsonSR = cJSON_GetObjectItem(jsonData, "sampleRate");
    //       sampleRate = jsonSR && jsonSR->valueint ? jsonSR->valueint : 0;

    //       switch(sampleRate) {
    //         case 8000:
    //           strcpy(fileType, ".r8");
    //           break;
    //         case 16000:
    //           strcpy(fileType, ".r16");
    //           break;
    //         case 24000:
    //           strcpy(fileType, ".r24");
    //           break;
    //         case 32000:
    //           strcpy(fileType, ".r32");
    //           break;
    //         case 48000:
    //           strcpy(fileType, ".r48");
    //           break;
    //         case 64000:
    //           strcpy(fileType, ".r64");
    //           break;
    //         default:
    //           strcpy(fileType, ".r16");
    //           break;
    //       }
    //     }
    //     else if (0 == strcmp(szAudioContentType, "wave") || 0 == strcmp(szAudioContentType, "wav")) {
    //       strcpy(fileType, ".wav");
    //     }
    //     else {
    //       validAudio = 0;
    //       switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "(%u) processIncomingMessage - unsupported audioContentType: %s\n", tech_pvt->id, szAudioContentType);
    //     }

    //     if (validAudio) {
    //       char szFilePath[256];

    //       std::string rawAudio = norwood::base64_decode(jsonAudio->valuestring);
    //       switch_snprintf(szFilePath, 256, "%s%s%s_%d.tmp%s", SWITCH_GLOBAL_dirs.temp_dir, 
    //         SWITCH_PATH_SEPARATOR, tech_pvt->sessionId, playCount++, fileType);
    //       std::ofstream f(szFilePath, std::ofstream::binary);
    //       f << rawAudio;
    //       f.close();

    //       // add the file to the list of files played for this session, we'll delete when session closes
    //       struct playout* playout = (struct playout *) malloc(sizeof(struct playout));
    //       playout->file = (char *) malloc(strlen(szFilePath) + 1);
    //       strcpy(playout->file, szFilePath);
    //       playout->next = tech_pvt->playout;
    //       tech_pvt->playout = playout;

    //       jsonFile = cJSON_CreateString(szFilePath);
    //       cJSON_AddItemToObject(jsonData, "file", jsonFile);
    //     }

    //     char* jsonString = cJSON_PrintUnformatted(jsonData);
    //     tech_pvt->responseHandler(session, EVENT_PLAY_AUDIO, jsonString);
    // }
    // else if (0 == type.compare("killAudio")) {
    //   tech_pvt->responseHandler(session, EVENT_KILL_AUDIO, NULL);

    //   // kill any current playback on the channel
    //   switch_channel_t *channel = switch_core_session_get_channel(session);
    //   switch_channel_set_flag_value(channel, CF_BREAK, 2);
    // }
    // else if (0 == type.compare("transcription")) {
    //   char* jsonString = cJSON_PrintUnformatted(jsonData);
    //   tech_pvt->responseHandler(session, EVENT_TRANSCRIPTION, jsonString);
    //   free(jsonString);        
    // }
    // else if (0 == type.compare("transfer")) {
    //   char* jsonString = cJSON_PrintUnformatted(jsonData);
    //   tech_pvt->responseHandler(session, EVENT_TRANSFER, jsonString);
    //   free(jsonString);                
    // }
    // else if (0 == type.compare("disconnect")) {
    //   char* jsonString = cJSON_PrintUnformatted(jsonData);
    //   tech_pvt->responseHandler(session, EVENT_DISCONNECT, jsonString);
    //   free(jsonString);        
    // }
    // else if (0 == type.compare("error")) {
    //   char* jsonString = cJSON_PrintUnformatted(jsonData);
    //   tech_pvt->responseHandler(session, EVENT_ERROR, jsonString);
    //   free(jsonString);        
    // }
    // else if (0 == type.compare("json")) {
    //   char* jsonString = cJSON_PrintUnformatted(json);
    //   tech_pvt->responseHandler(session, EVENT_JSON, jsonString);
    //   free(jsonString);
    // }
    // else {
    //   switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_ERROR, "(%u) processIncomingMessage - unsupported msg type %s\n", tech_pvt->id, type.c_str());  
    // }
  }

  static void eventCallback(const char* sessionId, AudioPipe::NotifyEvent_t event, const char* message, size_t msg_length) {
    switch_core_session_t* session = switch_core_session_locate(sessionId);
    if (session) {
      switch_channel_t *channel = switch_core_session_get_channel(session);
      switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
      if (bug) {
        private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
        if (tech_pvt) {
          switch (event) {
            case AudioPipe::CONNECT_SUCCESS:
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "eventCallback - connection successful - sessionId: %s message:%s\n", sessionId, message);
              tech_pvt->responseHandler(session, EVENT_CONNECT_SUCCESS, NULL);
              
              //We are connected and ready for transcription; let's flush audio buffer
              switch_core_media_bug_flush(bug);

              if (strlen(tech_pvt->initialMetadata) > 0) {
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "sending initial metadata %s\n", tech_pvt->initialMetadata);
                AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
                pAudioPipe->bufferForSending(tech_pvt->initialMetadata);
              }
            break;
            
            case AudioPipe::CONNECT_FAIL:
            {
              // first thing: we can no longer access the AudioPipe
              std::stringstream json;
              json << "{\"reason\":\"" << message << "\"}";
              tech_pvt->pAudioPipe = nullptr;
              tech_pvt->responseHandler(session, EVENT_CONNECT_FAIL, (char *) json.str().c_str());
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "eventCallback - connection failed: %s\n", message);
            }
            break;
            case AudioPipe::CONNECTION_DROPPED:
              // first thing: we can no longer access the AudioPipe
              tech_pvt->pAudioPipe = nullptr;
              tech_pvt->responseHandler(session, EVENT_DISCONNECT, NULL);
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "eventCallback - connection dropped from far end\n");
            break;
            case AudioPipe::CONNECTION_CLOSED_GRACEFULLY:
              // first thing: we can no longer access the AudioPipe
              tech_pvt->pAudioPipe = nullptr;
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "eventCallback - connection closed gracefully\n");
            break;
            case AudioPipe::MESSAGE:
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "eventCallback MESSAGE - sessionId: %s message:%s, len:%4d\n", sessionId, message, msg_length);
              processIncomingMessage(tech_pvt, session,"MESSAGE", message, msg_length);
            break;
            case AudioPipe::AUDIO:
              switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "eventCallback AUDIO - sessionId: %s message:%s, len:%4d\n", sessionId, message, msg_length);
              processIncomingMessage(tech_pvt, session,"AUDIO", message, msg_length);

            break;
          }
        }
      }
      switch_core_session_rwunlock(session);
    }
  }
  switch_status_t audio_docker_data_init(private_t *tech_pvt, switch_core_session_t *session, char * host, 
    unsigned int port, char* path, int sslFlags, int sampling, int desiredSampling, int channels, char* metadata, responseHandler_t responseHandler) {

    const char* api_token = nullptr;
    int err;
    switch_codec_implementation_t read_impl;
    switch_channel_t *channel = switch_core_session_get_channel(session);

    switch_core_session_get_read_impl(session, &read_impl);

    memset(tech_pvt, 0, sizeof(private_t));
  
    strncpy(tech_pvt->sessionId, switch_core_session_get_uuid(session), MAX_SESSION_ID);
    strncpy(tech_pvt->host, host, MAX_WS_URL_LEN);
    tech_pvt->port = port;
    strncpy(tech_pvt->path, path, MAX_PATH_LEN);    
    tech_pvt->sampling = desiredSampling;
    tech_pvt->responseHandler = responseHandler;
    tech_pvt->playout = NULL;
    tech_pvt->channels = channels;
    tech_pvt->id = ++idxCallCount;
    tech_pvt->buffer_overrun_notified = 0;
    tech_pvt->audio_paused = 0; //1; // pause audio until we get connected and get response from the far end
    tech_pvt->graceful_shutdown = 0;
    
    if (metadata) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "audio_docker_data_init - coping metadata\n");
      strncpy(tech_pvt->initialMetadata, metadata, MAX_METADATA_LEN);
    } else {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "audio_docker_data_init - NO metadata\n");
    }
    size_t buflen = LWS_PRE + (streaming_size * 2 );

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "audio_docker_data_init - desiredSampling:%d,nAudioBufferSecs:%u decoded_bytes_per_packet:%u, buflen: %u \n",desiredSampling,nAudioBufferSecs,read_impl.decoded_bytes_per_packet, buflen);
    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "ech_pvt->sampling:%d,tech_pvt->path: %s, host:%s, port:%d,tech_pvt->sessionId:%s, bufferlen:%d\n",tech_pvt->sampling, path, host,port,tech_pvt->sessionId, buflen);

    AudioPipe* ap = new AudioPipe(tech_pvt->sessionId, host, port, path, sslFlags, 
      buflen, read_impl.decoded_bytes_per_packet,metadata, eventCallback);
    if (!ap) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error allocating AudioPipe\n");
      return SWITCH_STATUS_FALSE;
    }

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "AudioPipe is created.\n");
    tech_pvt->pAudioPipe = static_cast<void *>(ap);

    switch_mutex_init(&tech_pvt->mutex, SWITCH_MUTEX_NESTED, switch_core_session_get_pool(session));

    if (desiredSampling != sampling) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) resampling from %u to %u\n", tech_pvt->id, sampling, desiredSampling);
      tech_pvt->resampler = speex_resampler_init(channels, sampling, desiredSampling, SWITCH_RESAMPLE_QUALITY, &err);
      if (0 != err) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "Error initializing resampler: %s.\n", speex_resampler_strerror(err));
        return SWITCH_STATUS_FALSE;
      }
    }
    else {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "(%u) no resampling needed for this call\n", tech_pvt->id);
    }

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "(%u) audio_docker_data_init\n", tech_pvt->id);

    return SWITCH_STATUS_SUCCESS;
  }

  void destroy_tech_pvt(private_t* tech_pvt) {
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_INFO, "%s (%u) destroy_tech_pvt\n", tech_pvt->sessionId, tech_pvt->id);
    if (tech_pvt->resampler) {
      speex_resampler_destroy(tech_pvt->resampler);
      tech_pvt->resampler = nullptr;
    }
    if (tech_pvt->mutex) {
      switch_mutex_destroy(tech_pvt->mutex);
      tech_pvt->mutex = nullptr;
    }
  }

  void lws_logger(int level, const char *line) {
    switch_log_level_t llevel = SWITCH_LOG_DEBUG;

    switch (level) {
      case LLL_ERR: llevel = SWITCH_LOG_ERROR; break;
      case LLL_WARN: llevel = SWITCH_LOG_WARNING; break;
      case LLL_NOTICE: llevel = SWITCH_LOG_NOTICE; break;
      case LLL_INFO: llevel = SWITCH_LOG_INFO; break;
      break;
    }
	  switch_log_printf(SWITCH_CHANNEL_LOG, llevel, "%s\n", line);
  }
}

extern "C" {
  int parse_ws_uri(switch_channel_t *channel, const char* szServerUri, char* host, char *path, unsigned int* pPort, int* pSslFlags) {
    int i = 0, offset;
    char server[MAX_WS_URL_LEN + MAX_PATH_LEN];
    char *saveptr;
    int flags = LCCSCF_USE_SSL;
    
    // get the scheme
    strncpy(server, szServerUri, MAX_WS_URL_LEN + MAX_PATH_LEN);
    if (0 == strncmp(server, "https://", 8) || 0 == strncmp(server, "HTTPS://", 8)) {
      *pSslFlags = flags;
      offset = 8;
      *pPort = 443;
    }
    else if (0 == strncmp(server, "wss://", 6) || 0 == strncmp(server, "WSS://", 6)) {
      *pSslFlags = flags;
      offset = 6;
      *pPort = 443;
    }
    else if (0 == strncmp(server, "http://", 7) || 0 == strncmp(server, "HTTP://", 7)) {
      offset = 7;
      *pSslFlags = 0;
      *pPort = 80;
    }
    else if (0 == strncmp(server, "ws://", 5) || 0 == strncmp(server, "WS://", 5)) {
      offset = 5;
      *pSslFlags = 0;
      *pPort = 80;
    }
    else {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - error parsing uri %s: invalid scheme\n", szServerUri);;
      return 0;
    }

    std::string strHost(server + offset);
    std::regex re("^(.+?):?(\\d+)?(/.*)?$");
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "strHost: %s\n", strHost.c_str());
    std::smatch matches;
    if(std::regex_search(strHost, matches, re)) {
      /*
      for (int i = 0; i < matches.length(); i++) {
        switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - %d: %s\n", i, matches[i].str().c_str());
      }
      */
      strncpy(host, matches[1].str().c_str(), MAX_WS_URL_LEN);
      if (matches[2].str().length() > 0) {
        *pPort = atoi(matches[2].str().c_str());
      }
      if (matches[3].str().length() > 0) {
        strncpy(path, matches[3].str().c_str(), MAX_PATH_LEN);
      }
      else {
        strcpy(path, "/");
      }
    } else {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - invalid format %s\n", strHost.c_str());
      return 0;
    }
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "parse_ws_uri - host: %s, path: %s\n", host, path);

    return 1;
  }

  switch_status_t audio_docker_init() {
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_docker: audio buffer (in secs):    %d secs\n", nAudioBufferSecs);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_docker: sub-protocol:              %s\n", mySubProtocolName);
    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "mod_audio_docker: lws service threads:       %d\n", nServiceThreads);
 
    int logs = LLL_ERR | LLL_WARN | LLL_NOTICE ;
     //LLL_INFO | LLL_PARSER | LLL_HEADER | LLL_EXT | LLL_CLIENT  | LLL_LATENCY | LLL_DEBUG ;
    AudioPipe::initialize(mySubProtocolName, nServiceThreads, logs, lws_logger);
   return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t audio_docker_cleanup() {
    bool cleanup = false;
    cleanup = AudioPipe::deinitialize();
    if (cleanup == true) {
        return SWITCH_STATUS_SUCCESS;
    }
    return SWITCH_STATUS_FALSE;
  }

  switch_status_t audio_docker_session_init(switch_core_session_t *session, 
              responseHandler_t responseHandler,
              uint32_t samples_per_second, 
              char *host,
              unsigned int port,
              char *path,
              int sampling,
              int sslFlags,
              int channels,
              char* metadata,
              void **ppUserData)
  {    	
    int err;

    // allocate per-session data structure
    private_t* tech_pvt = (private_t *) switch_core_session_alloc(session, sizeof(private_t));
    if (!tech_pvt) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "error allocating memory!\n");
      return SWITCH_STATUS_FALSE;
    }

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "audio_docker_session_init - samples_per_second:%u,sampling:%d,channels:%d, metadata: %s \n", samples_per_second,sampling, channels, metadata);

    if (SWITCH_STATUS_SUCCESS != audio_docker_data_init(tech_pvt, session, host, port, path, sslFlags, samples_per_second, sampling, channels, metadata, responseHandler)) {
      destroy_tech_pvt(tech_pvt);
      return SWITCH_STATUS_FALSE;
    }

    *ppUserData = tech_pvt;

    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
    pAudioPipe->connect();
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t audio_docker_session_cleanup(switch_core_session_t *session, char* text, int channelIsClosing) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "audio_docker_session_cleanup: no bug - websocket conection already closed\n");
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
    uint32_t id = tech_pvt->id;

    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) audio_docker_session_cleanup\n", id);

    if (!tech_pvt) return SWITCH_STATUS_FALSE;
    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
      
    switch_mutex_lock(tech_pvt->mutex);

    // get the bug again, now that we are under lock
    {
      switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
      if (bug) {
        switch_channel_set_private(channel, MY_BUG_NAME, NULL);
        if (!channelIsClosing) {
          switch_core_media_bug_remove(session, &bug);
        }
      }
    }

    // delete any temp files
    struct playout* playout = tech_pvt->playout;
    while (playout) {
      std::remove(playout->file);
      free(playout->file);
      struct playout *tmp = playout;
      playout = playout->next;
      free(tmp);
    }
    clear_playback_sequence(tech_pvt->sessionId);

    if (pAudioPipe && text) pAudioPipe->bufferForSending(text);
    if (pAudioPipe) pAudioPipe->close();

    destroy_tech_pvt(tech_pvt);
    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "(%u) audio_docker_session_cleanup: connection closed\n", id);
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t audio_docker_session_send_text(switch_core_session_t *session, char* text) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "audio_docker_session_send_text failed because no bug\n");
      return SWITCH_STATUS_FALSE;
    }
    switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_INFO, "audio_docker_session_send_text: %s\n", text);

    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
  
    if (!tech_pvt) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "audio_docker_session_send_text failed because no tech_pvt\n");
      return SWITCH_STATUS_FALSE;
    }
    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
    if (pAudioPipe && text) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "audio_docker_session_send_text - len: %d\n",strlen(text));
       pAudioPipe->bufferForSending(text);
    } else {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "audio_docker_session_send_text failed to get AudioPipe\n");
      return SWITCH_STATUS_FALSE;

    }
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t audio_docker_session_pauseresume(switch_core_session_t *session, int pause) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "audio_docker_session_pauseresume failed because no bug\n");
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
  
    if (!tech_pvt) return SWITCH_STATUS_FALSE;

    switch_core_media_bug_flush(bug);
    tech_pvt->audio_paused = pause;
    return SWITCH_STATUS_SUCCESS;
  }

  switch_status_t audio_docker_session_graceful_shutdown(switch_core_session_t *session) {
    switch_channel_t *channel = switch_core_session_get_channel(session);
    switch_media_bug_t *bug = (switch_media_bug_t*) switch_channel_get_private(channel, MY_BUG_NAME);
    if (!bug) {
      switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "audio_docker_session_graceful_shutdown failed because no bug\n");
      return SWITCH_STATUS_FALSE;
    }
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
  
    if (!tech_pvt) return SWITCH_STATUS_FALSE;

    tech_pvt->graceful_shutdown = 1;

    AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
    if (pAudioPipe) pAudioPipe->do_graceful_shutdown();

    return SWITCH_STATUS_SUCCESS;
  }

  switch_bool_t audio_docker_frame(switch_core_session_t *session, switch_media_bug_t *bug) {
    private_t* tech_pvt = (private_t*) switch_core_media_bug_get_user_data(bug);
    size_t inuse = 0;
    bool dirty = false;
    char *p = (char *) "{\"msg\": \"buffer overrun\"}";

    if (!tech_pvt || tech_pvt->audio_paused || tech_pvt->graceful_shutdown) {
      if (skip_printing++ % 100 == 0) {
        switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_NOTICE, "audio_docker_frame - return - audio paused or shutdown\n");
        if (skip_printing >= 100) skip_printing = 0;
      }
      return SWITCH_TRUE;
    }
 
    if (switch_mutex_trylock(tech_pvt->mutex) == SWITCH_STATUS_SUCCESS) {
      if (!tech_pvt->pAudioPipe) {
        switch_mutex_unlock(tech_pvt->mutex);
        return SWITCH_TRUE;
      }
      AudioPipe *pAudioPipe = static_cast<AudioPipe *>(tech_pvt->pAudioPipe);
      if (pAudioPipe->getLwsState() != AudioPipe::LWS_CLIENT_CONNECTED) {
        switch_mutex_unlock(tech_pvt->mutex);
        return SWITCH_TRUE;
      }

      pAudioPipe->lockAudioBuffer();
      size_t available = pAudioPipe->binarySpaceAvailable();
      if (NULL == tech_pvt->resampler) {
        switch_frame_t frame = { 0 };
        frame.data = pAudioPipe->binaryWritePtr();
        frame.buflen = available;
        while (true) {

          // check if buffer would be overwritten; dump packets if so
          if (available < pAudioPipe->binaryMinSpace()) {
            if (!tech_pvt->buffer_overrun_notified) {
              tech_pvt->buffer_overrun_notified = 1;
              tech_pvt->responseHandler(session, EVENT_BUFFER_OVERRUN, NULL);
            }
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "(%u) dropping packets!\n", 
              tech_pvt->id);
            pAudioPipe->binaryWritePtrResetToZero();

            frame.data = pAudioPipe->binaryWritePtr();
            frame.buflen = available = pAudioPipe->binarySpaceAvailable();
          }

          switch_status_t rv = switch_core_media_bug_read(bug, &frame, SWITCH_TRUE);
          if (rv != SWITCH_STATUS_SUCCESS)  {
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "audio_docker_frame - exit while(true)\n");
            break;
          }
          if (frame.datalen) {
            pAudioPipe->binaryWritePtrAdd(frame.datalen);
            frame.buflen = available = pAudioPipe->binarySpaceAvailable();
            frame.data = pAudioPipe->binaryWritePtr();
            dirty = true;
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "(%u) audio_docker_frame - sessionId:%s write audio len:%d, audio_buffer_size:%d\n", tech_pvt->id, switch_core_session_get_uuid(session), frame.datalen, (pAudioPipe->binarySpaceSize() - LWS_PRE));
          }
        }
      }
      else {
        uint8_t data[SWITCH_RECOMMENDED_BUFFER_SIZE];
        switch_frame_t frame = { 0 };
        frame.data = data;
        frame.buflen = SWITCH_RECOMMENDED_BUFFER_SIZE;
        while (switch_core_media_bug_read(bug, &frame, SWITCH_TRUE) == SWITCH_STATUS_SUCCESS) {
          if (frame.datalen) {
            spx_uint32_t out_len = available >> 1;  // space for samples which are 2 bytes
            spx_uint32_t in_len = frame.samples;

            speex_resampler_process_interleaved_int(tech_pvt->resampler, 
              (const spx_int16_t *) frame.data, 
              (spx_uint32_t *) &in_len, 
              (spx_int16_t *) ((char *) pAudioPipe->binaryWritePtr()),
              &out_len);

            if (out_len > 0) {
              // bytes written = num samples * 2 * num channels
              size_t bytes_written = out_len << tech_pvt->channels;
              pAudioPipe->binaryWritePtrAdd(bytes_written);
              available = pAudioPipe->binarySpaceAvailable();
              dirty = true;
            }
            if (available < pAudioPipe->binaryMinSpace()) {
              if (!tech_pvt->buffer_overrun_notified) {
                tech_pvt->buffer_overrun_notified = 1;
                switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_ERROR, "(%u) dropping packets!\n", 
                  tech_pvt->id);
                tech_pvt->responseHandler(session, EVENT_BUFFER_OVERRUN, NULL);
              }
              break;
            }
          }
        }
      }

      pAudioPipe->unlockAudioBuffer();
      switch_mutex_unlock(tech_pvt->mutex);
    }
    return SWITCH_TRUE;
  }

}
