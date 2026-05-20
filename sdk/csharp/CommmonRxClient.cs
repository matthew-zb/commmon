using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Commmon.Sdk
{
    /// <summary>
    /// 수신 데이터 구조체
    /// </summary>
    public record RxData
    {
        [JsonPropertyName("port")]
        public string Port { get; init; } = "";

        [JsonPropertyName("timestamp")]
        public string Timestamp { get; init; } = "";

        [JsonPropertyName("ascii")]
        public string Ascii { get; init; } = "";

        [JsonPropertyName("hex")]
        public string Hex { get; init; } = "";
    }

    /// <summary>
    /// commmon 데몬에 TCP 접속하여 실시간 RX 데이터를 수신하는 경량 클라이언트.
    /// 외부 의존성 없이 System.Net.Sockets + System.Text.Json만 사용.
    /// </summary>
    public class CommmonRxClient : IDisposable
    {
        private TcpClient? _tcp;
        private NetworkStream? _stream;
        private StreamWriter? _writer;
        private Thread? _recvThread;
        private volatile bool _running;

        /// <summary>실시간 RX 데이터 수신 이벤트</summary>
        public event Action<RxData>? OnData;

        /// <summary>오류 발생 이벤트</summary>
        public event Action<string>? OnError;

        /// <summary>
        /// 데몬에 TCP 접속
        /// </summary>
        public async Task ConnectAsync(string host = "127.0.0.1", int port = 9900)
        {
            _tcp = new TcpClient();
            await _tcp.ConnectAsync(host, port);
            _stream = _tcp.GetStream();
            _writer = new StreamWriter(_stream, new UTF8Encoding(false)) { AutoFlush = true };

            _running = true;
            _recvThread = new Thread(ReceiveLoop) { IsBackground = true };
            _recvThread.Start();
        }

        /// <summary>
        /// 포트의 실시간 RX 구독 시작
        /// </summary>
        public async Task SubscribeAsync(string comPort)
        {
            await SendCommandAsync("subscribe_rx", new { port = comPort });
        }

        /// <summary>
        /// 포트의 실시간 RX 구독 해제
        /// </summary>
        public async Task UnsubscribeAsync(string comPort)
        {
            await SendCommandAsync("unsubscribe_rx", new { port = comPort });
        }

        /// <summary>
        /// 연결 종료
        /// </summary>
        public void Disconnect()
        {
            _running = false;
            _stream?.Close();
            _tcp?.Close();
        }

        public void Dispose()
        {
            Disconnect();
            GC.SuppressFinalize(this);
        }

        private async Task SendCommandAsync(string cmd, object args)
        {
            if (_writer == null) throw new InvalidOperationException("연결되지 않았습니다.");

            var msg = JsonSerializer.Serialize(new { cmd, args });
            await _writer.WriteLineAsync(msg);
        }

        private void ReceiveLoop()
        {
            try
            {
                using var reader = new StreamReader(_stream!, Encoding.UTF8);
                while (_running)
                {
                    var line = reader.ReadLine();
                    if (line == null) break;

                    line = line.Trim();
                    if (string.IsNullOrEmpty(line)) continue;

                    try
                    {
                        using var doc = JsonDocument.Parse(line);
                        var root = doc.RootElement;

                        // notification 확인
                        if (root.TryGetProperty("notify", out var notifyProp) &&
                            notifyProp.GetString() == "rx_data" &&
                            root.TryGetProperty("data", out var dataProp))
                        {
                            var rxData = JsonSerializer.Deserialize<RxData>(dataProp.GetRawText());
                            if (rxData != null)
                            {
                                OnData?.Invoke(rxData);
                            }
                        }
                    }
                    catch
                    {
                        // JSON 파싱 오류 무시 (응답 메시지 등)
                    }
                }
            }
            catch (Exception ex)
            {
                if (_running)
                {
                    OnError?.Invoke(ex.Message);
                }
            }
        }
    }
}
