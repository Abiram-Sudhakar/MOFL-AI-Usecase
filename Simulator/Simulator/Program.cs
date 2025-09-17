using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;

class MarketSimulator
{
    static Random rnd = new Random();
    static Dictionary<string, Order> orderBook = new();
    static List<string> symbols = new() { "AAPL", "GOOG", "MSFT" };
    static int batchSize = 20; // number of messages per batch

    static void Main()
    {
        int port = 5000;
        TcpListener listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        Console.WriteLine($"Waiting for client connection on port {port}...");

        TcpClient client = null;
        NetworkStream stream = null;

        while (true)
        {
            try
            {
                if (client == null || !client.Connected)
                {
                    client = listener.AcceptTcpClient();
                    stream = client.GetStream();
                    Console.WriteLine("Client connected!");
                }

                List<string> batch = new List<string>();
                Dictionary<string, Order> updatesThisBatch = new();

                // Generate inserts
                for (int i = 0; i < 5; i++)
                {
                    var order = GenerateOrder();
                    orderBook[order.OrderId] = order;
                    updatesThisBatch[order.OrderId] = order;

                    string msgId = Guid.NewGuid().ToString();
                    string csv = $"INSERT,{msgId},{DateTime.UtcNow:o},{order.ToCsv()}";
                    batch.Add(csv);
                    Console.WriteLine(csv);
                }

                // Random updates (coalesced)
                foreach (var kvp in orderBook)
                {
                    if (rnd.NextDouble() < 0.3) // 30% chance to update
                    {
                        var order = kvp.Value;
                        order.Price += Math.Round((decimal)(rnd.NextDouble() - 0.5) * 2, 2);
                        updatesThisBatch[order.OrderId] = order;
                    }
                }

                // Add coalesced updates to batch
                foreach (var order in updatesThisBatch.Values)
                {
                    string msgId = Guid.NewGuid().ToString();
                    string csv = $"UPDATE,{msgId},{DateTime.UtcNow:o},{order.ToCsv()}";
                    batch.Add(csv);
                    Console.WriteLine(csv);
                }

                // Random deletes
                List<string> deletes = new();
                foreach (var kvp in orderBook)
                {
                    if (rnd.NextDouble() < 0.05)
                    {
                        deletes.Add(kvp.Key);
                        string msgId = Guid.NewGuid().ToString();
                        string csv = $"DELETE,{msgId},{DateTime.UtcNow:o},{kvp.Value.OrderId}";
                        batch.Add(csv);
                        Console.WriteLine(csv);
                    }
                }
                foreach (var id in deletes)
                    orderBook.Remove(id);

                // Generate correlated trades
                foreach (var order in orderBook.Values)
                {
                    if (rnd.NextDouble() < 0.3) // 30% chance to generate trade
                    {
                        var trade = GenerateTrade(order);
                        string msgId = Guid.NewGuid().ToString();
                        string csv = $"TRADE,{msgId},{DateTime.UtcNow:o},{trade.ToCsv()}";
                        batch.Add(csv);
                        Console.WriteLine(csv);
                    }
                }

                // Send batch over TCP
                foreach (var msg in batch)
                    SendMessage(stream, msg);

                Thread.Sleep(50); // simulate market update rate
            }
            catch (Exception ex)
            {
                Console.WriteLine("Connection lost: " + ex.Message);
                client?.Close();
                client = null;
                stream = null;
                Thread.Sleep(1000); // retry after 1s
            }
        }
    }

    static Order GenerateOrder()
    {
        string orderId = Guid.NewGuid().ToString();
        string symbol = symbols[rnd.Next(symbols.Count)];
        string side = rnd.NextDouble() < 0.5 ? "BUY" : "SELL";
        decimal price = Math.Round((decimal)(rnd.NextDouble() * 1000 + 10), 2);
        int qty = rnd.Next(1, 1000);
        return new Order { OrderId = orderId, Symbol = symbol, Side = side, Price = price, Quantity = qty, Timestamp = DateTime.UtcNow };
    }

    static Trade GenerateTrade(Order order)
    {
        return new Trade
        {
            TradeId = Guid.NewGuid().ToString(),
            Symbol = order.Symbol,
            Buyer = order.Side == "BUY" ? "TraderX" : "TraderA",
            Seller = order.Side == "SELL" ? "TraderY" : "TraderB",
            TradePrice = order.Price,
            TradeQty = Math.Min(order.Quantity, rnd.Next(1, 500)),
            Timestamp = DateTime.UtcNow
        };
    }

    static void SendMessage(NetworkStream stream, string msg)
    {
        byte[] data = Encoding.UTF8.GetBytes(msg + "\n");
        stream.Write(data, 0, data.Length);
    }
}

class Order
{
    public string OrderId, Symbol, Side;
    public decimal Price;
    public int Quantity;
    public DateTime Timestamp;

    public string ToCsv() => $"{OrderId},{Symbol},{Side},{Price},{Quantity},{Timestamp:o}";
}

class Trade
{
    public string TradeId, Symbol, Buyer, Seller;
    public decimal TradePrice;
    public int TradeQty;
    public DateTime Timestamp;

    public string ToCsv() => $"{TradeId},{Symbol},{Buyer},{Seller},{TradePrice},{TradeQty},{Timestamp:o}";
}
