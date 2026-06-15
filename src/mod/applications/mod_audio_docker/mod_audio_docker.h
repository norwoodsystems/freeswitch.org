#ifndef __MOD_AUDIO_DOCKER_H__
#define __MOD_AUDIO_DOCKER_H__

#include <switch.h>
#include <libwebsockets.h>
#include <speex/speex_resampler.h>

#include <unistd.h>

#define MY_BUG_NAME "audio_docker"
#define MAX_SESSION_ID (256)
#define MAX_WS_URL_LEN (512)
#define MAX_PATH_LEN (4096)

#define EVENT_DISCONNECT      "mod_audio_docker::disconnect"
#define EVENT_ERROR           "mod_audio_docker::error"
#define EVENT_CONNECT_SUCCESS "mod_audio_docker::connect"
#define EVENT_CONNECT_FAIL    "mod_audio_docker::connect_failed"
#define EVENT_BUFFER_OVERRUN  "mod_audio_docker::buffer_overrun"
#define EVENT_PLAY_AUDIO      "mod_audio_docker::play_audio"
#define EVENT_KILL_AUDIO      "mod_audio_docker::kill_audio"
#define EVENT_JSON            "mod_audio_docker::json"
#define EVENT_TEXT_MESSAGE    "mod_audio_docker::text_message"

#define PLAY_AUDIO_TO_A_LEG    "A"
#define PLAY_AUDIO_TO_B_LEG    "B"
#define PLAY_AUDIO_TO_BOTH     "AB"
#define DISPLACE_AUDIO         "YES"

#define MAX_METADATA_LEN (8192)

struct playout {
  char *file;
  struct playout* next;
};

typedef void (*responseHandler_t)(switch_core_session_t* session, const char* eventName, char* json);

struct private_data {
	switch_mutex_t *mutex;
	char sessionId[MAX_SESSION_ID];
  SpeexResamplerState *resampler;
  responseHandler_t responseHandler;
  void *pAudioPipe;
  int ws_state;
  char host[MAX_WS_URL_LEN];
  unsigned int port;
  char path[MAX_PATH_LEN];
  int sampling;
  struct playout* playout;
  int  channels;
  unsigned int id;
  int buffer_overrun_notified:1;
  int audio_paused:1;
  int graceful_shutdown:1;
  char initialMetadata[8533];
  switch_queue_t   *audio_queue;
  switch_thread_t  *playback_thread;
  volatile int      playback_running;
  unsigned int      audio_seq;
};

typedef struct private_data private_t;

#endif
