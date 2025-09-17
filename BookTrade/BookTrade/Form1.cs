using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System;
using System.Threading.Tasks;
using System.Windows.Forms;
using System;
using System.IO;
using System.Linq;
using System.Timers;
using System;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace BookTrade
{
    public partial class Form1 : Form
    {
        private readonly ConcurrentQueue<string> messageQueue = new();
        private readonly ColumnStore orderBook = new(7);   // 7 fields as in simulator
        private readonly ColumnStore tradeBook = new(7);   // adjust for trade fields
        private TcpClient client;
        private NetworkStream stream;

        private DataGridView dgvOrders;
        private DataGridView dgvTrades;

        public Form1()
        {
            InitializeComponent();
            CreateGrids();

            StartReceiver();
            StartBatchProcessor();
        }

        private void CreateGrids()
        {
            // OrderBook Grid
            dgvOrders = new DataGridView
            {
                Dock = DockStyle.Top,
                Height = this.ClientSize.Height / 2,
                VirtualMode = true,
                AllowUserToAddRows = false,
                AllowUserToDeleteRows = false,
                RowHeadersVisible = false,
                ReadOnly = true
            };
            dgvOrders.CellValueNeeded += (s, e) =>
            {
                e.Value = orderBook.GetValue(e.RowIndex, e.ColumnIndex);
            };
            for (int i = 0; i < 7; i++)
                dgvOrders.Columns.Add($"Col{i}", $"Order Col {i}");

            // TradeBook Grid
            dgvTrades = new DataGridView
            {
                Dock = DockStyle.Fill,
                VirtualMode = true,
                AllowUserToAddRows = false,
                AllowUserToDeleteRows = false,
                RowHeadersVisible = false,
                ReadOnly = true
            };
            dgvTrades.CellValueNeeded += (s, e) =>
            {
                e.Value = tradeBook.GetValue(e.RowIndex, e.ColumnIndex);
            };
            for (int i = 0; i < 7; i++)
                dgvTrades.Columns.Add($"Col{i}", $"Trade Col {i}");

            // Add to form
            this.Controls.Add(dgvTrades);
            this.Controls.Add(dgvOrders);
        }

        private void StartReceiver()
        {
            Task.Run(async () =>
            {
                while (true)
                {
                    try
                    {
                        client = new TcpClient("127.0.0.1", 5000);
                        stream = client.GetStream();
                        byte[] buffer = new byte[8192];

                        while (client.Connected)
                        {
                            int bytes = await stream.ReadAsync(buffer, 0, buffer.Length);
                            if (bytes > 0)
                            {
                                string data = Encoding.UTF8.GetString(buffer, 0, bytes);
                                foreach (var line in data.Split('\n', StringSplitOptions.RemoveEmptyEntries))
                                    messageQueue.Enqueue(line);
                            }
                        }
                    }
                    catch
                    {
                        await Task.Delay(1000); // retry if disconnected
                    }
                }
            });
        }

        private void StartBatchProcessor()
        {
            System.Windows.Forms.Timer timer = new System.Windows.Forms.Timer { Interval = 33 }; // ~30 fps
            timer.Tick += (s, e) =>
            {
                int processed = 0;
                while (messageQueue.TryDequeue(out var msg))
                {
                    ApplyMessage(msg);
                    processed++;
                }

                if (processed > 0)
                {
                    dgvOrders.RowCount = orderBook.RowCount;
                    dgvTrades.RowCount = tradeBook.RowCount;
                    dgvOrders.Invalidate();
                    dgvTrades.Invalidate();
                }
            };
            timer.Start();
        }

        private void ApplyMessage(string msg)
        {
            if (string.IsNullOrWhiteSpace(msg))
                return;

            var parts = msg.Split(',', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 3)
            {
                Console.WriteLine($"⚠️ Invalid message: {msg}");
                return;
            }

            string type = parts[0];
            string msgId = parts[1];

            DateTime sendTs;
            if (!DateTime.TryParse(parts[2], null,
                System.Globalization.DateTimeStyles.RoundtripKind,
                out sendTs))
            {
                sendTs = DateTime.UtcNow; // fallback
            }

            var now = DateTime.UtcNow;

            try
            {
                switch (type)
                {
                    case "INSERT":
                        if (parts.Length >= 9) // MsgId + SendTs + 6 fields
                            orderBook.Insert(parts[3], parts[3..]);
                        break;

                    case "UPDATE":
                        if (parts.Length >= 9)
                            orderBook.Update(parts[3], parts[3..]);
                        break;

                    case "DELETE":
                        if (parts.Length >= 4)
                            orderBook.Delete(parts[3]);
                        break;

                    case "TRADE":
                        if (parts.Length >= 7)
                            tradeBook.Insert(parts[3], parts[3..]);
                        break;

                    default:
                        Console.WriteLine($"⚠️ Unknown type: {type}");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error applying message {msg}: {ex.Message}");
            }

            double latencyMs = (now - sendTs).TotalMilliseconds;
            Console.WriteLine($"Msg {msgId} ({type}) latency {latencyMs:F2} ms");
        }

    }

    class ColumnStore
    {
        private readonly System.Collections.Generic.List<string[]> rows = new();
        private readonly System.Collections.Generic.Dictionary<string, int> rowIndex = new();
        private readonly int colsCount;

        public ColumnStore(int cols)
        {
            colsCount = cols;
        }

        public int RowCount => rows.Count;

        public string GetValue(int row, int col)
        {
            if (row < 0 || row >= rows.Count) return "";
            var arr = rows[row];
            if (col < 0 || col >= arr.Length) return "";
            return arr[col];
        }

        public void Insert(string id, string[] cols)
        {
            rowIndex[id] = rows.Count;
            rows.Add(cols);
        }

        public void Update(string id, string[] cols)
        {
            if (rowIndex.TryGetValue(id, out int idx))
                rows[idx] = cols;
        }

        public void Delete(string id)
        {
            if (rowIndex.TryGetValue(id, out int idx))
            {
                rows[idx] = new string[colsCount]; // blank row
                rowIndex.Remove(id);
            }
        }
    }
}
