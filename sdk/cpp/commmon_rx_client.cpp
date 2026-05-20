#include "commmon_rx_client.h"
#include <cstdio>
#include <cstring>

static bool g_wsaInitialized = false;

static void EnsureWSA() {
    if (!g_wsaInitialized) {
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
        g_wsaInitialized = true;
    }
}

CommmonRxClient::CommmonRxClient()
    : m_socket(INVALID_SOCKET)
    , m_thread(NULL)
    , m_running(false)
    , m_callback(nullptr)
{
    EnsureWSA();
}

CommmonRxClient::~CommmonRxClient() {
    Disconnect();
}

bool CommmonRxClient::Connect(const char* host, int port) {
    m_socket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (m_socket == INVALID_SOCKET) return false;

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((u_short)port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(m_socket, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        closesocket(m_socket);
        m_socket = INVALID_SOCKET;
        return false;
    }

    m_running = true;
    m_thread = CreateThread(NULL, 0, RecvThreadProc, this, 0, NULL);
    return true;
}

void CommmonRxClient::SetCallback(RxDataCallback cb) {
    m_callback = cb;
}

void CommmonRxClient::SetCallback(
    std::function<void(const char*, const char*, const char*, const char*)> cb) {
    m_funcCallback = cb;
}

bool CommmonRxClient::Subscribe(const char* comPort) {
    return SendCommand("subscribe_rx", comPort);
}

bool CommmonRxClient::Unsubscribe(const char* comPort) {
    return SendCommand("unsubscribe_rx", comPort);
}

void CommmonRxClient::Disconnect() {
    m_running = false;
    if (m_socket != INVALID_SOCKET) {
        closesocket(m_socket);
        m_socket = INVALID_SOCKET;
    }
    if (m_thread) {
        WaitForSingleObject(m_thread, 3000);
        CloseHandle(m_thread);
        m_thread = NULL;
    }
}

bool CommmonRxClient::SendCommand(const char* cmd, const char* portArg) {
    if (m_socket == INVALID_SOCKET) return false;

    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"cmd\":\"%s\",\"args\":{\"port\":\"%s\"}}\n", cmd, portArg);

    int len = (int)strlen(buf);
    return send(m_socket, buf, len, 0) == len;
}

DWORD WINAPI CommmonRxClient::RecvThreadProc(LPVOID param) {
    auto* self = (CommmonRxClient*)param;
    self->RecvLoop();
    return 0;
}

void CommmonRxClient::RecvLoop() {
    std::string buffer;
    char chunk[4096];

    while (m_running) {
        int n = recv(m_socket, chunk, sizeof(chunk) - 1, 0);
        if (n <= 0) break;

        chunk[n] = '\0';
        buffer += chunk;

        size_t pos;
        while ((pos = buffer.find('\n')) != std::string::npos) {
            std::string line = buffer.substr(0, pos);
            buffer.erase(0, pos + 1);

            // 앞뒤 공백 제거
            while (!line.empty() && (line.front() == ' ' || line.front() == '\r'))
                line.erase(line.begin());
            while (!line.empty() && (line.back() == ' ' || line.back() == '\r'))
                line.pop_back();

            if (!line.empty()) {
                ParseNotification(line);
            }
        }
    }
}

void CommmonRxClient::ParseNotification(const std::string& line) {
    // "notify":"rx_data" 확인
    if (line.find("\"notify\"") == std::string::npos) return;
    if (line.find("\"rx_data\"") == std::string::npos) return;

    // "data" 블록에서 필드 추출
    std::string port = ExtractJsonString(line, "port");
    std::string timestamp = ExtractJsonString(line, "timestamp");
    std::string ascii = ExtractJsonString(line, "ascii");
    std::string hex = ExtractJsonString(line, "hex");

    if (m_callback) {
        m_callback(port.c_str(), timestamp.c_str(), ascii.c_str(), hex.c_str());
    }
    if (m_funcCallback) {
        m_funcCallback(port.c_str(), timestamp.c_str(), ascii.c_str(), hex.c_str());
    }
}

std::string CommmonRxClient::ExtractJsonString(const std::string& json,
                                                const std::string& key) {
    // 간단한 JSON 문자열 값 추출: "key":"value"
    std::string pattern = "\"" + key + "\":\"";
    size_t start = json.find(pattern);
    if (start == std::string::npos) return "";

    start += pattern.length();
    std::string result;
    for (size_t i = start; i < json.length(); i++) {
        if (json[i] == '\\' && i + 1 < json.length()) {
            // 이스케이프 시퀀스 처리
            i++;
            switch (json[i]) {
                case '"':  result += '"'; break;
                case '\\': result += '\\'; break;
                case 'n':  result += '\n'; break;
                case 'r':  result += '\r'; break;
                case 't':  result += '\t'; break;
                default:   result += json[i]; break;
            }
        } else if (json[i] == '"') {
            break;
        } else {
            result += json[i];
        }
    }
    return result;
}
