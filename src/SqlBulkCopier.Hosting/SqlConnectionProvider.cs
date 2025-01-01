using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;

namespace SqlBulkCopier.Hosting
{
    public class SqlConnectionProvider(IConfiguration configuration)
    {
        // 必要に応じてメソッドで接続文字列を公開
        public async Task<SqlConnection> OpenAsync(string name = "DefaultConnection")
        {
            // appsettings.json などで定義された接続文字列を取得
            var connectionString = configuration.GetConnectionString(name);

            if (connectionString is null)
            {
                throw new InvalidOperationException($"Connection string '{name}' is not found or empty.");
            }

            var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            return connection;
        }
    }
}