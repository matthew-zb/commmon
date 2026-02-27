#include "commmon_rx_client.h"
#include <cstdio>

// commmon 데몬 실시간 RX 수신 예제
// 빌드: cl /EHsc example.cpp commmon_rx_client.cpp /link ws2_32.lib
// 사전 조건: commmon daemon 실행 중, COM 포트가 열려 있어야 함

int main() {
    CommmonRxClient client;

    client.SetCallback([](const char* port, const char* ts,
                          const char* ascii, const char* hex) {
        printf("[%s] [%s] %s\n", ts, port, ascii);
    });

    if (!client.Connect("127.0.0.1", 9900)) {
        fprintf(stderr, "데몬 접속 실패\n");
        return 1;
    }
    printf("데몬 접속 완료\n");

    client.Subscribe("COM14");
    printf("COM14 구독 시작. Enter 키를 누르면 종료합니다.\n");

    getchar();

    client.Unsubscribe("COM14");
    client.Disconnect();
    printf("종료\n");

    return 0;
}
