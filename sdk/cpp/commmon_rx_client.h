#pragma once
/**
 * commmon RX 실시간 수신 클라이언트 (C++ / Winsock2)
 * 외부 의존성 없음. JSON 라이브러리 불필요 (간단한 문자열 파싱).
 */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <string>
#include <functional>

#pragma comment(lib, "ws2_32.lib")

/// RX 데이터 콜백 함수 타입
typedef void (*RxDataCallback)(const char* port, const char* timestamp,
                                const char* ascii, const char* hex);

class CommmonRxClient {
public:
    CommmonRxClient();
    ~CommmonRxClient();

    /// 데몬에 TCP 접속
    bool Connect(const char* host = "127.0.0.1", int port = 9900);

    /// 포트 실시간 RX 구독
    bool Subscribe(const char* comPort);

    /// 포트 실시간 RX 구독 해제
    bool Unsubscribe(const char* comPort);

    /// 콜백 설정 (Connect 전에 호출)
    void SetCallback(RxDataCallback cb);

    /// std::function 콜백 설정
    void SetCallback(std::function<void(const char*, const char*,
                                         const char*, const char*)> cb);

    /// 연결 종료
    void Disconnect();

private:
    SOCKET m_socket;
    HANDLE m_thread;
    volatile bool m_running;
    RxDataCallback m_callback;
    std::function<void(const char*, const char*, const char*, const char*)> m_funcCallback;

    bool SendCommand(const char* cmd, const char* portArg);
    static DWORD WINAPI RecvThreadProc(LPVOID param);
    void RecvLoop();
    void ParseNotification(const std::string& line);
    static std::string ExtractJsonString(const std::string& json, const std::string& key);
};
