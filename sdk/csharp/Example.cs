using Commmon.Sdk;

// commmon 데몬 실시간 RX 수신 예제
// 사전 조건: commmon daemon 실행 중, COM 포트가 열려 있어야 함

var client = new CommmonRxClient();
client.OnData += data => Console.WriteLine($"[{data.Timestamp}] [{data.Port}] {data.Ascii}");
client.OnError += err => Console.Error.WriteLine($"오류: {err}");

await client.ConnectAsync("127.0.0.1", 9900);
Console.WriteLine("데몬 접속 완료");

await client.SubscribeAsync("COM14");
Console.WriteLine("COM14 구독 시작. Enter 키를 누르면 종료합니다.");

Console.ReadLine();

await client.UnsubscribeAsync("COM14");
client.Disconnect();
Console.WriteLine("종료");
