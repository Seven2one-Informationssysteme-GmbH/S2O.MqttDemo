namespace S2O.MqttDemo;

using System;

public static class Log
{
    private static readonly object SyncLock = new();

    public static void Info(string message)
        => Write(ConsoleColor.Gray, message);

    public static void Output(string message)
        => Write(ConsoleColor.White, message);

    public static void Warning(string message)
        => Write(ConsoleColor.Yellow, message);

    public static void Warning(Exception exception, string message)
        => Write(ConsoleColor.Yellow, message, exception);

    public static void Error(Exception exception, string message)
        => Write(ConsoleColor.Red, message, exception);

    private static void Write(ConsoleColor color, string message, Exception? exception = null)
    {
        lock (SyncLock)
        {
            var oldColor = Console.ForegroundColor;
            try
            {
                Console.ForegroundColor = color;

                Console.WriteLine(message);

                if (exception is not null)
                {
                    Console.WriteLine($"{exception.GetType().Name}: {exception.Message}");
                    Console.WriteLine(exception.StackTrace);
                }
            }
            finally
            {
                Console.ForegroundColor = oldColor;
            }
        }
    }
}

