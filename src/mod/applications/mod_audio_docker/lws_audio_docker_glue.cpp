#include <switch.h>
#include <switch_json.h>
#include <string.h>
#include <string>
#include <mutex>
#include <thread>
#include <list>
#include <algorithm>
#include <functional>
#include <cassert>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <regex>
#include <stdio.h>
#include <unistd.h>
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
  static const char *freeswitchHome = std::getenv("HOME") ? std::getenv("HOME") : "/usr/local/freswitch"; 
  static const char *audioDockerServer = std::getenv("MOD_AUDIO_DOCKER_SERVER") ? std::getenv("MOD_AUDIO_DOCKER_SERVER") : "localhost:8080"; 
  static const char* mySubProtocolName = std::getenv("MOD_AUDIO_DOCKER_SUBPROTOCOL_NAME") ?
    std::getenv("MOD_AUDIO_DOCKER_SUBPROTOCOL_NAME") : "streaming";
  static unsigned int nServiceThreads = std::max(1, std::min(requestedNumServiceThreads ? ::atoi(requestedNumServiceThreads) : 1, 5));
  static unsigned int idxCallCount = 0;
  static uint32_t playCount = 0;
  static uint32_t skip_printing = 0;
  static size_t streaming_size = FRAME_SIZE_8000 * ::atoi(numberOfFramesForStreaming);


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

  void processIncomingMessage(private_t* tech_pvt, switch_core_session_t* session, const char* msg_type, const char* message, size_t length) {
  std::string msg = message;
  std::string type  = msg_type;

    // switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "(%u) processIncomingMessage - received msg_type:%s, message:%s \n", tech_pvt->id, type, msg);
    if (!session) {
      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - unable to find session\n");
    } else {
      if (type == "AUDIO") {
        // Let's try both options - stream audio to the session and and to a callig party as well as store it in a file and then send it to the session
          const char* sessionId = switch_core_session_get_uuid(session);
          lwsl_notice("processIncomingMessage - (%s) AUDIO (len:%d)\n",sessionId, length);
          if (session && switch_channel_ready(switch_core_session_get_channel(session))) {
            unsigned char header[44] = {0};
            memcpy(header, message, 44);
            parse_wav_header(header);
            // std::string filename = "";
            // filename = strcat((char*)sessionId,".wav");
            // std::string path =  strcat((char*)freeswitchHome, "/");
            std::string filename = std::string(sessionId) + ".wav";
            std::string path =  std::string(freeswitchHome) + "/" + filename;
            
            FILE* file = fopen(path.c_str(), "wb");
            size_t written = fwrite(message, sizeof(char), length, file);
            fclose(file);
            switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - store audio into file: %s message-len:%d\n",path.c_str(), length);
            if (written != length) {
                // Handle partial write or error
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_NOTICE, "processIncomingMessage - Failed to write all audio data - written: %d, len: %d\n", written, length);
            }
            int displace = 0;
            if (strcmp(displaceAudio, DISPLACE_AUDIO) == 0) {
              displace = 1;
            }

            if (strcmp(playAudioDirection, PLAY_AUDIO_TO_A_LEG) == 0) {
              switch_status_t status = SWITCH_STATUS_NOT_INITALIZED;
              if (displace == 1) {
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - switch_ivr_displace_session\n");
                status = switch_ivr_displace_session(session, path.c_str(), 0, NULL);
              } else {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - play the announcement on A leg of the call.");
                status = switch_ivr_play_file(session, NULL, path.c_str(), NULL);
              }
              if (status != SWITCH_STATUS_SUCCESS) {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - Failed to play audio file: %s\n", path.c_str());
              } else {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - Played audio file: %s\n", path.c_str());
                  // Delete the file
                  if (std::remove(path.c_str()) == 0) {
                    // free(file);
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - The file %s was deleted successfully.\n", path.c_str());
                  } else {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,  "processIncomingMessage - Error deleting the file");
                  }
              }
            } else if (strcmp(playAudioDirection, PLAY_AUDIO_TO_B_LEG) == 0) {
              switch_channel_t *channel = switch_core_session_get_channel(session);
              const char *other_uuid = switch_channel_get_variable(channel, SWITCH_BRIDGE_UUID_VARIABLE);
              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - other_uuid: %s\n", other_uuid);
              switch_core_session_t *other_session = switch_core_session_locate(other_uuid);
              if (other_session) {
                switch_status_t status = SWITCH_STATUS_NOT_INITALIZED;
                if (displace == 1) {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - play the announcement on B leg of the call.");
                  status = switch_ivr_displace_session(other_session, path.c_str(), 0, NULL);
                } else {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - switch_ivr_play_file");
                  status = switch_ivr_play_file(other_session, NULL, path.c_str(), NULL);
                }
                if (status != SWITCH_STATUS_SUCCESS) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - Failed to play audio file: %s\n", path.c_str());
                } else {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - Played audio file: %s\n", path.c_str());
                    // Delete the file
                    if (std::remove(path.c_str()) == 0) {
                      // free(file);
                      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - The file %s was deleted successfully.\n", path.c_str());
                    } else {
                      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,  "processIncomingMessage (bridged_session) - Error deleting the file");
                    }
                }
              } else {
                      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,  "processIncomingMessage (bridged_session) - Could not locate (bridged_session)");
              }
            } else if (strcmp(playAudioDirection, PLAY_AUDIO_TO_BOTH) == 0) {
              switch_channel_t *channel = switch_core_session_get_channel(session);
              const char *other_uuid = switch_channel_get_variable(channel, SWITCH_BRIDGE_UUID_VARIABLE);
              switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - other_uuid: %s\n", other_uuid);
              switch_core_session_t *other_session = switch_core_session_locate(other_uuid);
              if (other_session) {
                switch_status_t status = SWITCH_STATUS_NOT_INITALIZED;
                switch_status_t status1 = SWITCH_STATUS_NOT_INITALIZED;
                if (displace == 1) {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - play the announcement on both legs of the call.");
                  status = switch_ivr_displace_session(session, path.c_str(), 0, NULL);
                  status1 = switch_ivr_displace_session(other_session, path.c_str(), 0, NULL);
                } else {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage - switch_ivr_play_file");
                  status = switch_ivr_play_file(other_session, NULL, path.c_str(), NULL);
                }
                if (status != SWITCH_STATUS_SUCCESS) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (session) - Failed to play audio file: %s\n", path.c_str());
                } 
                if (status1 != SWITCH_STATUS_SUCCESS) {
                    switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (bridged_session) - Failed to play audio file: %s\n", path.c_str());
                } 
                switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (PLAY_AUDIO_TO_BOTH) - Played audio file: %s\n", path.c_str());
                // Delete the file
                if (std::remove(path.c_str()) == 0) {
                  // free(file);
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG, "processIncomingMessage (PLAY_AUDIO_TO_BOTH) - The file %s was deleted successfully.\n", path.c_str());
                } else {
                  switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_DEBUG,  "processIncomingMessage (PLAY_AUDIO_TO_BOTH) - Error deleting the file");
                }
              } else {
                      switch_log_printf(SWITCH_CHANNEL_LOG, SWITCH_LOG_WARNING,  "processIncomingMessage (PLAY_AUDIO_TO_BOTH)  - Could not locate (bridged_session)");
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
        tech_pvt->responseHandler(session, EVENT_TEXT_MESSAGE, (char *) msg.c_str());
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
            switch_log_printf(SWITCH_CHANNEL_SESSION_LOG(session), SWITCH_LOG_DEBUG, "audio_docker_frame - write audio len:%d, audio_buffer_size:%d\n", frame.datalen, (pAudioPipe->binarySpaceSize() - LWS_PRE));
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

