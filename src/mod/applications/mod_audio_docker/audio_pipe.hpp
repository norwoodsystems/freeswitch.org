#ifndef __AUDIO_PIPE_HPP__
#define __AUDIO_PIPE_HPP__

#include <string>
#include <list>
#include <mutex>

#include <libwebsockets.h>

#define RTP_PACKETIZATION_PERIOD 20
#define FRAME_SIZE_8000  320 /*which means each 20ms frame as 320 bytes at 8 khz (1 channel only)*/
#define AUDIO_DOCKER_FRAME_SIZE  FRAME_SIZE_8000  * 15 /*which means each 150ms*/

class AudioPipe {
public:
  enum LwsState_t {
    LWS_CLIENT_IDLE,
    LWS_CLIENT_CONNECTING,
    LWS_CLIENT_CONNECTED,
    LWS_CLIENT_FAILED,
    LWS_CLIENT_DISCONNECTING,
    LWS_CLIENT_DISCONNECTED
  };
  enum NotifyEvent_t {
    CONNECT_SUCCESS,
    CONNECT_FAIL,
    CONNECTION_DROPPED,
    CONNECTION_CLOSED_GRACEFULLY,
    MESSAGE,
    AUDIO
  };
  typedef void (*log_emit_function)(int level, const char *line);
  typedef void (*notifyHandler_t)(const char *sessionId, NotifyEvent_t event, const char* message, size_t msg_length);

  struct lws_per_vhost_data {
    struct lws_context *context;
    struct lws_vhost *vhost;
    const struct lws_protocols *protocol;
  };

  static void initialize(const char* protocolName, unsigned int nThreads, int loglevel, log_emit_function logger);
  static bool deinitialize();
  static bool lws_service_thread(unsigned int nServiceThread);

  // constructor
  AudioPipe(const char* uuid, const char* host, unsigned int port, const char* path, int sslFlags, 
    size_t bufLen, size_t minFreespace, const char* metadata, notifyHandler_t callback);
  ~AudioPipe();  

  LwsState_t getLwsState(void) { return m_state; }
  void connect(void);
  void bufferForSending(const char* text);
  size_t binarySpaceAvailable(void) {
    return m_audio_buffer_max_len - m_audio_buffer_write_offset;
  }
  size_t binarySpaceSize(void) {
    return m_audio_buffer_write_offset;
  }
  size_t binaryMinSpace(void) {
    return m_audio_buffer_min_freespace;
  }
  char * binaryWritePtr(void) { 
    return (char *) m_audio_buffer + m_audio_buffer_write_offset;
  }
  char * audioReadPtr(void) { 
    return (char *) m_audio_buffer;
  }
  void binaryWritePtrAdd(size_t len) {
    m_audio_buffer_write_offset += len;
  }
 
  void binaryWritePtrResetToZero(void) {
    m_audio_buffer_write_offset = LWS_PRE;
  }
  void clearMetadata(void) {
    m_metadata.clear();
  }
  std::string getMetadata() {
    return m_metadata;
  }

  void lockAudioBuffer(void) {
    m_audio_mutex.lock();
  }
  std::string base64EncodedAudio(size_t len);

  char* b64AudioEncoding(size_t len);

  void unlockAudioBuffer(void) ;

  void do_graceful_shutdown();
  bool isGracefulShutdown(void) {
    return m_gracefulShutdown;
  }

  bool isAudioDetected(void) {
    return m_audio_detected;
  }
  bool isSilenceDetected(void) {
    return m_silence_detected;
  }
  void audioDetected(bool value) {
    m_audio_detected = value;
  }
  void silenceDetected(bool value) {
    m_silence_detected = value;
  }

  void close() ;

  int64_t getSilenceStartTime(void)
  {
    return m_silence_start_time;
  }

  void storeSilenceStartTime(int64_t time)
  {
    m_silence_start_time = time;
  }
  FILE* getAudioTTSFile(void)
  {
    return m_audio_TTS_file;
  }
  void setAudioTTSFile(FILE* file)
  {
    m_audio_TTS_file = file;
  }
  unsigned int getAudioTTSChunkSize(void)
  {
    return m_audio_TTS_chunk_size;
  }
  void setAudioTTSChunkSize(unsigned int size)
  {
    m_audio_TTS_chunk_size = size;
  }

  // no default constructor or copying
  AudioPipe() = delete;
  AudioPipe(const AudioPipe&) = delete;
  void operator=(const AudioPipe&) = delete;

private:

  static int lws_callback(struct lws *wsi, enum lws_callback_reasons reason, void *user, void *in, size_t len); 
  static bool lws_initialized;
  static bool lws_stopping;
  static unsigned int nchild;
  static struct lws_context *contexts[];
  static unsigned int numContexts;
  static std::string protocolName;
  static std::mutex mutex_connects;
  static std::mutex mutex_disconnects;
  static std::mutex mutex_writes;
  static std::list<AudioPipe*> pendingConnects;
  static std::list<AudioPipe*> pendingDisconnects;
  static std::list<AudioPipe*> pendingWrites;
  static log_emit_function logger;

  static AudioPipe* findAndRemovePendingConnect(struct lws *wsi);
  static AudioPipe* findPendingConnect(struct lws *wsi);
  static void addPendingConnect(AudioPipe* ap);
  static void addPendingDisconnect(AudioPipe* ap);
  static void addPendingWrite(AudioPipe* ap);
  static void processPendingConnects(lws_per_vhost_data *vhd);
  static void processPendingDisconnects(lws_per_vhost_data *vhd);
  static void processPendingWrites(void);
  
  bool connect_client(struct lws_per_vhost_data *vhd);

  LwsState_t m_state;
  std::string m_uuid;
  std::string m_host;
  unsigned int m_port;
  std::string m_path;
  std::string m_metadata;
  std::string m_debug_metadata;
  std::mutex m_text_mutex;
  std::mutex m_audio_mutex;
  int m_sslFlags;
  struct lws *m_wsi;
  uint8_t *m_audio_buffer;
  size_t m_audio_buffer_max_len;
  size_t m_audio_buffer_write_offset;
  size_t m_audio_buffer_min_freespace;
  uint8_t* m_recv_buf;
  uint8_t* m_recv_buf_ptr;
  size_t m_recv_buf_len;
  struct lws_per_vhost_data* m_vhd;
  notifyHandler_t m_callback;
  log_emit_function m_logger;
  std::string m_api_token;
  bool m_gracefulShutdown;
 	int64_t m_silence_start_time;
  bool m_silence_detected;
  bool m_audio_detected;
  FILE* m_audio_TTS_file;
  unsigned int m_audio_TTS_chunk_size;
};

#endif
