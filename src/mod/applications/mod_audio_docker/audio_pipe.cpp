#include "audio_pipe.hpp"
#include "base64.hpp"
#include <switch.h>

#include <thread>
#include <cassert>
#include <iostream>
#include <sstream>
#include <vector>

/* discard incoming text messages over the socket that are longer than this */
#define MAX_RECV_BUF_SIZE (65 * 1024 * 10)
#define RECV_BUF_REALLOC_SIZE (8 * 1024)


namespace {

  static const char *requestedTcpKeepaliveSecs = std::getenv("MOD_AUDIO_DOCKER_TCP_KEEPALIVE_SECS");
  static int nTcpKeepaliveSecs = requestedTcpKeepaliveSecs ? ::atoi(requestedTcpKeepaliveSecs) : 55;
}
  static const char* apiToken = std::getenv("MOD_AUDIO_DOCKER_TOKEN");
  static const char *numberOfFramesForTranscription = std::getenv("MOD_AUDIO_DOCKER_FRAME_SIZE");
  size_t streaming_size = FRAME_SIZE_8000 * ::atoi(numberOfFramesForTranscription);



// remove once we update to lws with this helper
static int dch_lws_http_basic_auth_gen(const char *user, const char *pw, char *buf, size_t len) {
	size_t n = strlen(user), m = strlen(pw);
	char b[128];

	if (len < 6 + ((4 * (n + m + 1)) / 3) + 1)
		return 1;

	memcpy(buf, "Basic ", 6);

	n = lws_snprintf(b, sizeof(b), "%s:%s", user, pw);
	if (n >= sizeof(b) - 2)
		return 2;

	lws_b64_encode_string(b, n, buf + 6, len - 6);
	buf[len - 1] = '\0';

	return 0;
}

int AudioPipe::lws_callback(struct lws *wsi, 
  enum lws_callback_reasons reason,
  void *user, void *in, size_t len) {

  struct AudioPipe::lws_per_vhost_data *vhd = 
    (struct AudioPipe::lws_per_vhost_data *) lws_protocol_vh_priv_get(lws_get_vhost(wsi), lws_get_protocol(wsi));

  struct lws_vhost* vhost = lws_get_vhost(wsi);
  AudioPipe ** ppAp = (AudioPipe **) user;

  switch (reason) {
    case LWS_CALLBACK_PROTOCOL_INIT:
      lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_PROTOCOL_INIT\n");
      vhd = (struct AudioPipe::lws_per_vhost_data *) lws_protocol_vh_priv_zalloc(lws_get_vhost(wsi), lws_get_protocol(wsi), sizeof(struct AudioPipe::lws_per_vhost_data));
      lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_PROTOCOL_INIT-1\n");
      vhd->context = lws_get_context(wsi);
      lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_PROTOCOL_INI-2\n");
      vhd->protocol = lws_get_protocol(wsi);
      lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_PROTOCOL_INIT-3\n");
      vhd->vhost = lws_get_vhost(wsi);
      lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_PROTOCOL_INIT-4\n");
      break;

    case LWS_CALLBACK_CLIENT_APPEND_HANDSHAKE_HEADER:
      {
        AudioPipe* ap = findPendingConnect(wsi);

          unsigned char **p = (unsigned char **)in, *end = (*p) + len;
          char b[128];

          memcpy(b, apiToken,strlen(apiToken));
          b[strlen(apiToken)] = '\0';
          lwsl_notice("AudioPipe::lws_callback  HANDSHAKE_HEADER Authorization: %s\n",b);
          lwsl_notice("AudioPipe::lws_callback  HANDSHAKE_HEADER Authorization - token: %s, metadata:%s\n",apiToken, ap->getMetadata().c_str());
          if (lws_add_http_header_by_token(wsi, WSI_TOKEN_HTTP_AUTHORIZATION, (unsigned char *)b, strlen(b), p, end)) return -1;
          
          if (strstr(ap->getMetadata().c_str(),"call-id")) {
            // Add header by name
            std::stringstream ss(ap->getMetadata());
            std::string item;
            char delim = '/';
            char header_name[128];
            char header_value[128];
            char * token = NULL;
            char * next_token = NULL;

            while (std::getline(ss, item, delim)) {
              token = strtok((char *)item.c_str(),"=");
              next_token = strtok(NULL, "=");
              strcpy(header_name, token);
              strcpy(header_value, next_token);
              lwsl_notice("AudioPipe::lws_callback  HANDSHAKE_HEADER header: %s[len:%d], value:%s[len:%d]\n",header_name,strlen(header_name), header_value,strlen(header_value));
              if (lws_add_http_header_by_name(wsi, (unsigned char *)header_name, 
                                              (unsigned char *)header_value, strlen(header_value), p, end)) {
                  lwsl_err("Failed to add HTTP header by name - call-id\n");
                  return 1; // Error occurred
              }
		        }
          }
      }
      break;

    case LWS_CALLBACK_EVENT_WAIT_CANCELLED:
      lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_EVENT_WAIT_CANCELLED - processPendingWrites\n");
      processPendingConnects(vhd);
      processPendingDisconnects(vhd);
      processPendingWrites();
      break;
    case LWS_CALLBACK_CLIENT_CONNECTION_ERROR:
      {
        AudioPipe* ap = findAndRemovePendingConnect(wsi);
        if (ap) {
          lwsl_notice("AudioPipe::lws_callback - LWS_CALLBACK_CLIENT_CONNECTION_ERROR\n");
          ap->m_state = LWS_CLIENT_FAILED;
          ap->m_callback(ap->m_uuid.c_str(), AudioPipe::CONNECT_FAIL, (char *) in, 0);
        }
        else {
          lwsl_err("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_CONNECTION_ERROR unable to find wsi %p..\n", wsi); 
        }
      }      
      break;

    case LWS_CALLBACK_CLIENT_ESTABLISHED:
      {
        AudioPipe* ap = findAndRemovePendingConnect(wsi);
        if (ap) {
          *ppAp = ap;
          ap->m_vhd = vhd;
          ap->m_state = LWS_CLIENT_CONNECTED;
          lwsl_debug("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_ESTABLISHED - %s - calling a-->m_callback\n", ap->m_uuid.c_str()); 
          ap->m_callback(ap->m_uuid.c_str(), AudioPipe::CONNECT_SUCCESS, NULL,0);
        }
        else {
          lwsl_err("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_ESTABLISHED %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
        }
      }      
      break;
    case LWS_CALLBACK_CLIENT_CLOSED:
      {
         lwsl_notice("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_CLOSED \n"); 
        AudioPipe* ap = *ppAp;
        if (!ap) {
          lwsl_err("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_CLOSED %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
          return 0;
        }
        if (ap->m_state == LWS_CLIENT_DISCONNECTING) {
          // closed by us
          ap->m_callback(ap->m_uuid.c_str(), AudioPipe::CONNECTION_CLOSED_GRACEFULLY, NULL,0);
        }
        else if (ap->m_state == LWS_CLIENT_CONNECTED) {
          // closed by far end
          lwsl_notice("%s socket closed by far end\n", ap->m_uuid.c_str());
          ap->m_callback(ap->m_uuid.c_str(), AudioPipe::CONNECTION_DROPPED, NULL,0);
        }
        ap->m_state = LWS_CLIENT_DISCONNECTED;

        //NB: after receiving any of the events above, any holder of a 
        //pointer or reference to this object must treat is as no longer valid

        *ppAp = NULL;
        delete ap;
      }
      break;

    case LWS_CALLBACK_CLIENT_RECEIVE:
      {
        AudioPipe* ap = *ppAp;
        // lwsl_notice("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_RECEIVE\n");
        if (!ap) {
          lwsl_err("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_RECEIVE %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
          return 0;
        }
		    lwsl_notice("LWS_CALLBACK_CLIENT_RECEIVE: %4d (rpp: %5d, first: %d, last: %d, bin: %d)\n",
			  (int)len, (int)lws_remaining_packet_payload(wsi),
		    lws_is_first_fragment(wsi),
			  lws_is_final_fragment(wsi),
			  lws_frame_is_binary(wsi));

        if (lws_is_first_fragment(wsi)) {
          // allocate a buffer for the entire chunk of memory needed
          assert(nullptr == ap->m_recv_buf);
          ap->m_recv_buf_len = len + lws_remaining_packet_payload(wsi);
		      lwsl_notice("LWS_CALLBACK_CLIENT_RECEIVE - allocate recv_buf_len: %4d\n",ap->m_recv_buf_len);
          ap->m_recv_buf = (uint8_t*) malloc(ap->m_recv_buf_len);
          ap->m_recv_buf_ptr = ap->m_recv_buf;
        }

        size_t write_offset = ap->m_recv_buf_ptr - ap->m_recv_buf;
        size_t remaining_space = ap->m_recv_buf_len - write_offset;
        if (remaining_space < len) {
          lwsl_notice("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE buffer realloc needed.\n");
          size_t newlen = ap->m_recv_buf_len + RECV_BUF_REALLOC_SIZE;
          if (newlen > MAX_RECV_BUF_SIZE) {
            free(ap->m_recv_buf);
            ap->m_recv_buf = ap->m_recv_buf_ptr = nullptr;
            ap->m_recv_buf_len = 0;
            lwsl_notice("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_RECEIVE max buffer exceeded, truncating message.\n");
          }
          else {
            ap->m_recv_buf = (uint8_t*) realloc(ap->m_recv_buf, newlen);
            if (nullptr != ap->m_recv_buf) {
              ap->m_recv_buf_len = newlen;
              ap->m_recv_buf_ptr = ap->m_recv_buf + write_offset;
            }
          }
        }

        if (nullptr != ap->m_recv_buf) {
          if (len > 0) {
            memcpy(ap->m_recv_buf_ptr, in, len);
            ap->m_recv_buf_ptr += len;
          }
          if (lws_is_final_fragment(wsi)) {
            if (nullptr != ap->m_recv_buf) {
              std::string msg((char *)ap->m_recv_buf, ap->m_recv_buf_ptr - ap->m_recv_buf);
              if (lws_frame_is_binary(wsi)) {
		            lwsl_notice("LWS_CALLBACK_CLIENT_RECEIVE - before m_callback (binary - audio ) - m_recv_buf_len: %4d, copied: %4d\n",ap->m_recv_buf_len, (size_t)(ap->m_recv_buf_ptr - ap->m_recv_buf));
                ap->m_callback(ap->m_uuid.c_str(), AudioPipe::AUDIO, msg.c_str(), (size_t)(ap->m_recv_buf_ptr - ap->m_recv_buf));
              } else {
		            lwsl_notice("LWS_CALLBACK_CLIENT_RECEIVE - before m_callback (text - message ) - m_recv_buf_len: %4d, copied: %4d\n",ap->m_recv_buf_len, (size_t)(ap->m_recv_buf_ptr - ap->m_recv_buf));
                ap->m_callback(ap->m_uuid.c_str(), AudioPipe::MESSAGE, msg.c_str(),(size_t)(ap->m_recv_buf_ptr - ap->m_recv_buf) );
              }
              if (nullptr != ap->m_recv_buf) free(ap->m_recv_buf);
            }
            ap->m_recv_buf = ap->m_recv_buf_ptr = nullptr;
            ap->m_recv_buf_len = 0;
          }
        }
      }
      break;

    case LWS_CALLBACK_CLIENT_WRITEABLE:
      {
         lwsl_notice("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_WRITEABLE uuid:%s metadata:%s\n", (*ppAp) ? (*ppAp)->m_uuid.c_str() : "<nil>", (*ppAp) ? (*ppAp)->m_debug_metadata.c_str() : "<nil>"); 
        AudioPipe* ap = *ppAp;
        if (!ap) {
          lwsl_err("AudioPipe::lws_callback LWS_CALLBACK_CLIENT_WRITEABLE %s unable to find wsi %p..\n", ap->m_uuid.c_str(), wsi); 
          return 0;
        }

        // check for graceful close - send a zero length binary frame
        if (ap->isGracefulShutdown()) {
          lwsl_notice("%s graceful shutdown - sending zero length binary frame to flush any final responses\n", ap->m_uuid.c_str());
          std::lock_guard<std::mutex> lk(ap->m_audio_mutex);
          int sent = lws_write(wsi, (unsigned char *) ap->m_audio_buffer, 0, LWS_WRITE_BINARY);
          return 0;
        }

        // check for text frames to send
        {
          std::lock_guard<std::mutex> lk(ap->m_text_mutex);
          if (ap->m_metadata.length() > 0) {
            uint8_t buf[ap->m_metadata.length() + LWS_PRE];
            memcpy(buf + LWS_PRE, ap->m_metadata.c_str(), ap->m_metadata.length());
            int n = ap->m_metadata.length();
            int m = lws_write(wsi, buf + LWS_PRE, n, LWS_WRITE_TEXT);
            ap->m_metadata.clear();
            if (m < n) {
              lwsl_notice("AudioPipe::lws_write - CAN'T send all data m:%d < n:%d \n", m, n); 
              return -1;
            }

            lwsl_notice("AudioPipe::lws_write - uuid:%s metadata:%s sent %d text\n", ap->m_uuid.c_str(), ap->m_debug_metadata.c_str(), m); 
            // there may be audio data, but only one write per writeable event
            // get it next time
            lws_callback_on_writable(wsi);

            return 0;
          }
          else {
            lwsl_notice("AudioPipe::lws_write - uuid:%s metadata:%s NO DATA (text) to send\n", ap->m_uuid.c_str(), ap->m_debug_metadata.c_str()); 
          }
        }

        if (ap->m_state == LWS_CLIENT_DISCONNECTING) {
          lws_close_reason(wsi, LWS_CLOSE_STATUS_NORMAL, NULL, 0);
          return -1;
        }

        // check for audio packets
        {
          std::lock_guard<std::mutex> lk(ap->m_audio_mutex);
          if (ap->m_audio_buffer_write_offset > LWS_PRE) {
            size_t datalen = ap->m_audio_buffer_write_offset - LWS_PRE;
            int sent = lws_write(wsi, (unsigned char *) ap->m_audio_buffer + LWS_PRE, datalen, LWS_WRITE_BINARY);
            if (sent < datalen) {
              lwsl_err("AudioPipe::lws_service_thread LWS_CALLBACK_CLIENT_WRITEABLE %s attemped to send %lu only sent %d wsi %p..\n", 
                ap->m_uuid.c_str(), datalen, sent, wsi); 
            }
            lwsl_notice("AudioPipe::lws_write - uuid:%s metadata:%s sent %d audio(binary)\n", ap->m_uuid.c_str(), ap->m_debug_metadata.c_str(), sent); 
            ap->m_audio_buffer_write_offset = LWS_PRE;
          }
        }

        return 0;
      }
      break;

    default:
      break;
  }
  return lws_callback_http_dummy(wsi, reason, user, in, len);
}


// static members

struct lws_context *AudioPipe::contexts[] = {
  nullptr, nullptr, nullptr, nullptr, nullptr,
  nullptr, nullptr, nullptr, nullptr, nullptr
};
unsigned int AudioPipe::numContexts = 0;
bool AudioPipe::lws_initialized = false;
bool AudioPipe::lws_stopping = false;
unsigned int AudioPipe::nchild = 0;
std::string AudioPipe::protocolName;
std::mutex AudioPipe::mutex_connects;
std::mutex AudioPipe::mutex_disconnects;
std::mutex AudioPipe::mutex_writes;
std::list<AudioPipe*> AudioPipe::pendingConnects;
std::list<AudioPipe*> AudioPipe::pendingDisconnects;
std::list<AudioPipe*> AudioPipe::pendingWrites;
AudioPipe::log_emit_function AudioPipe::logger;

void AudioPipe::processPendingConnects(lws_per_vhost_data *vhd) {
  std::list<AudioPipe*> connects;
  {
    std::lock_guard<std::mutex> guard(mutex_connects);
    for (auto it = pendingConnects.begin(); it != pendingConnects.end(); ++it) {
      if ((*it)->m_state == LWS_CLIENT_IDLE) {
        connects.push_back(*it);
        (*it)->m_state = LWS_CLIENT_CONNECTING;
      }
    }
  }
  for (auto it = connects.begin(); it != connects.end(); ++it) {
    AudioPipe* ap = *it;
    ap->connect_client(vhd);   
  }
}

void AudioPipe::processPendingDisconnects(lws_per_vhost_data *vhd) {
  std::list<AudioPipe*> disconnects;
  {
    std::lock_guard<std::mutex> guard(mutex_disconnects);
    for (auto it = pendingDisconnects.begin(); it != pendingDisconnects.end(); ++it) {
      if ((*it)->m_state == LWS_CLIENT_DISCONNECTING) disconnects.push_back(*it);
    }
    pendingDisconnects.clear();
  }
  for (auto it = disconnects.begin(); it != disconnects.end(); ++it) {
    AudioPipe* ap = *it;
    lws_callback_on_writable(ap->m_wsi); 
  }
}

void AudioPipe::processPendingWrites() {
  std::list<AudioPipe*> writes;
  {
    std::lock_guard<std::mutex> guard(mutex_writes);
    for (auto it = pendingWrites.begin(); it != pendingWrites.end(); ++it) {
       if ((*it)->m_state == LWS_CLIENT_CONNECTED) {
        // lwsl_notice("AudioPipe-processPendingWrites move to writes\n"); 
         writes.push_back(*it);
       }
    }  
    pendingWrites.clear();
  }
  for (auto it = writes.begin(); it != writes.end(); ++it) {
    AudioPipe* ap = *it;
    // lwsl_notice("AudioPipe-processPendingWrites write to lws\n"); 
    lws_callback_on_writable(ap->m_wsi);
  }
}

AudioPipe* AudioPipe::findAndRemovePendingConnect(struct lws *wsi) {
  AudioPipe* ap = NULL;
  std::lock_guard<std::mutex> guard(mutex_connects);

  for (auto it = pendingConnects.begin(); it != pendingConnects.end() && !ap; ++it) {
    int state = (*it)->m_state;
    if ((state == LWS_CLIENT_CONNECTING) &&
      (*it)->m_wsi == wsi) ap = *it;
  }

  if (ap) {
    pendingConnects.remove(ap);
  }

  return ap;
}

AudioPipe* AudioPipe::findPendingConnect(struct lws *wsi) {
  AudioPipe* ap = NULL;
  std::lock_guard<std::mutex> guard(mutex_connects);

  for (auto it = pendingConnects.begin(); it != pendingConnects.end() && !ap; ++it) {
    int state = (*it)->m_state;
    if ((state == LWS_CLIENT_CONNECTING) &&
      (*it)->m_wsi == wsi) ap = *it;
  }
  return ap;
}

void AudioPipe::addPendingConnect(AudioPipe* ap) {
  {
    std::lock_guard<std::mutex> guard(mutex_connects);
    pendingConnects.push_back(ap);
    lwsl_notice("%s after adding connect there are %lu pending connects\n", 
      ap->m_uuid.c_str(), pendingConnects.size());
  }
  lws_cancel_service(contexts[nchild++ % numContexts]);
}
void AudioPipe::addPendingDisconnect(AudioPipe* ap) {
  ap->m_state = LWS_CLIENT_DISCONNECTING;
  {
    std::lock_guard<std::mutex> guard(mutex_disconnects);
    pendingDisconnects.push_back(ap);
    lwsl_notice("%s after adding disconnect there are %lu pending disconnects\n", 
      ap->m_uuid.c_str(), pendingDisconnects.size());
  }
  lws_cancel_service(ap->m_vhd->context);
}
void AudioPipe::addPendingWrite(AudioPipe* ap) {
    lwsl_notice("before - addPendingWrite uuid:%s metadata:%s\n", ap->m_uuid.c_str(), ap->m_debug_metadata.c_str()); 
  {
    std::lock_guard<std::mutex> guard(mutex_writes);
    pendingWrites.push_back(ap);
  }
  lws_cancel_service(ap->m_vhd->context);
}

bool AudioPipe::lws_service_thread(unsigned int nServiceThread) {
  struct lws_context_creation_info info;

  const struct lws_protocols protocols[] = {
    {
      protocolName.c_str(),
      AudioPipe::lws_callback,
      sizeof(void *),
      8553,
    },
    { NULL, NULL, 0, 0 }
  };

  memset(&info, 0, sizeof info); 
  info.port = CONTEXT_PORT_NO_LISTEN; 
  info.protocols = protocols;
  info.options = LWS_SERVER_OPTION_DO_SSL_GLOBAL_INIT;

  info.ka_time = nTcpKeepaliveSecs;                    // tcp keep-alive timer
  info.ka_probes = 4;                   // number of times to try ka before closing connection
  info.ka_interval = 5;                 // time between ka's
  info.timeout_secs = 10;                // doc says timeout for "various processes involving network roundtrips"
  info.keepalive_timeout = 5;           // seconds to allow remote client to hold on to an idle HTTP/1.1 connection 
  // info.ws_ping_pong_interval = 10;
  info.timeout_secs_ah_idle = 10;       // secs to allow a client to hold an ah without using it

  lwsl_notice("AudioPipe::lws_service_thread creating context in service thread %d.\n", nServiceThread);

  contexts[nServiceThread] = lws_create_context(&info);
  if (!contexts[nServiceThread]) {
    lwsl_err("AudioPipe::lws_service_thread failed creating context in service thread %d..\n", nServiceThread); 
    return false;
  }

  int n;
  do {
    n = lws_service(contexts[nServiceThread], 50);
  } while (n >= 0 && !lws_stopping);

  lwsl_notice("AudioPipe::lws_service_thread ending in service thread %d\n", nServiceThread); 
  return true;
}

void AudioPipe::initialize(const char* protocol, unsigned int nThreads, int loglevel, log_emit_function logger) {
  assert(!lws_initialized);
  assert(nThreads > 0 && nThreads <= 10);

  numContexts = nThreads;
  protocolName = protocol;
  lws_set_log_level(loglevel, logger);

  lwsl_notice("AudioPipe::initialize starting %d threads with subprotocol %s\n", nThreads, protocol); 
  for (unsigned int i = 0; i < numContexts; i++) {
    std::thread t(&AudioPipe::lws_service_thread, i);
    t.detach();
  }
  lws_initialized = true;
}

bool AudioPipe::deinitialize() {
  assert(lws_initialized);
  lwsl_notice("AudioPipe::deinitialize\n"); 
  lws_stopping = true;
  lws_initialized = false;
  do
  {
    lwsl_notice("waiting for pending connects to complete\n");
  } while (pendingConnects.size() > 0);
  do
  {
    lwsl_notice("waiting for disconnects to complete\n");
  } while (pendingDisconnects.size() > 0);

  for (unsigned int i = 0; i < numContexts; i++)
  {
    lwsl_notice("AudioPipe::deinitialize destroying context %d of %d\n", i + 1, numContexts);
    lws_context_destroy(contexts[i]);
  }
  return true;
}

// instance members
AudioPipe::AudioPipe(const char* uuid, const char* host, unsigned int port, const char* path,
  int sslFlags, size_t bufLen, size_t minFreespace, const char* metadata, notifyHandler_t callback) :
  m_uuid(uuid), m_host(host), m_port(port), m_path(path), m_sslFlags(sslFlags),
  m_audio_buffer_min_freespace(minFreespace), m_audio_buffer_max_len(bufLen), m_gracefulShutdown(false),
  m_silence_detected(false), m_audio_detected(false),
  m_audio_buffer_write_offset(LWS_PRE), m_recv_buf(nullptr), m_recv_buf_ptr(nullptr), 
  m_audio_TTS_file(nullptr),m_audio_TTS_chunk_size(0),
  m_state(LWS_CLIENT_IDLE), m_wsi(nullptr), m_vhd(nullptr), m_callback(callback) {

  m_audio_buffer = new uint8_t[m_audio_buffer_max_len];
  m_metadata.append(metadata);
  m_debug_metadata.append(metadata);
  if (apiToken) {
    m_api_token.assign(apiToken);
    lwsl_notice("AudioPipe:: init - uuid:%s apiToken:%s,metadata:%s, tcp_keepalive:%d, max_buffer_len:%d, min_freespace:%d\n", m_uuid.c_str(), apiToken,m_metadata.c_str(), nTcpKeepaliveSecs,m_audio_buffer_max_len, m_audio_buffer_min_freespace);
  }
}
AudioPipe::~AudioPipe() {
  if (m_audio_buffer) delete [] m_audio_buffer;
  if (m_recv_buf) delete [] m_recv_buf;
}

void AudioPipe::connect(void) {
  addPendingConnect(this);
}

bool AudioPipe::connect_client(struct lws_per_vhost_data *vhd) {
  assert(m_audio_buffer != nullptr);
  assert(m_vhd == nullptr);

  struct lws_client_connect_info i;

  memset(&i, 0, sizeof(i));
  i.context = vhd->context;
  i.port = m_port;
  i.address = m_host.c_str();
  i.path = m_path.c_str();
  i.host = i.address;
  i.origin = i.address;
  i.ssl_connection = m_sslFlags;
  i.protocol = protocolName.c_str();
  i.pwsi = &(m_wsi);
  i.method = NULL;

  m_state = LWS_CLIENT_CONNECTING;
  m_vhd = vhd;

  lwsl_notice("attempting connection - port:%d, address:%s,path:%s,host:%s, protocol:%s\n",i.port,i.address,i.path,i.host, i.protocol);
  m_wsi = lws_client_connect_via_info(&i);
  lwsl_notice("%s attempting connection, wsi is %p\n", m_uuid.c_str(), m_wsi);

  return nullptr != m_wsi;
}

void AudioPipe::bufferForSending(const char* text) {
  if (m_state != LWS_CLIENT_CONNECTED) return;
  {
    std::lock_guard<std::mutex> lk(m_text_mutex);
    m_metadata.append(text);
    lwsl_notice("bufferForSending - send_text: %s - length: %u\n",text, m_metadata.length());
    lwsl_notice("bufferForSending - send_text(m_metadata): %s - length: %u\n",m_metadata.c_str(), m_metadata.length());
  }
  addPendingWrite(this);
}

void AudioPipe::unlockAudioBuffer() {
  if (m_audio_buffer_write_offset > streaming_size) addPendingWrite(this);
  m_audio_mutex.unlock();
}

void AudioPipe::close() {
  if (m_state != LWS_CLIENT_CONNECTED) return;
  addPendingDisconnect(this);
}

void AudioPipe::do_graceful_shutdown() {
  m_gracefulShutdown = true;
  addPendingWrite(this);
}

std::string AudioPipe::base64EncodedAudio(size_t len) {
  return norwood::base64_encode((unsigned char*)audioReadPtr(), len);
}
char* AudioPipe::b64AudioEncoding(size_t len) {
  return norwood::b64_encode((unsigned char*)audioReadPtr(), len);
}
