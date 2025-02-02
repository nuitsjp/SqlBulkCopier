using System.Data;
using System.Globalization;
using System.Text;
using FixedLengthHelper;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Shouldly;
using SqlBulkCopier.FixedLength;
using SqlBulkCopier.FixedLength.Hosting;

namespace SqlBulkCopier.Test.FixedLength.Hosting;

public class FixedLengthBulkCopierParserTest
{
    private SqlConnection OpenConnection()
    {
        var connection = new SqlConnection(
            new SqlConnectionStringBuilder
            {
                DataSource = ".",
                InitialCatalog = "master",
                IntegratedSecurity = true,
                TrustServerCertificate = true
            }.ToString());
        connection.Open();
        return connection;
    }

    [Fact]
    public void Parse()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "HasHeader": true
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.DataReaderBuilder.ShouldBeOfType<FixedLengthDataReaderBuilder>();
    }

    [Fact]
    public void TruncateBeforeBulkInsert_True()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "TruncateBeforeBulkInsert": true
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.TruncateBeforeBulkInsert.ShouldBeTrue();
    }

    [Fact]
    public void TruncateBeforeBulkInsert_False()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "TruncateBeforeBulkInsert": false
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.TruncateBeforeBulkInsert.ShouldBeFalse();
    }

    [Fact]
    public void TruncateBeforeBulkInsert_NotSet()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.TruncateBeforeBulkInsert.ShouldBeFalse(); // Default value
    }

    [Fact]
    public void MaxRetryCount_SetCorrectly()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "MaxRetryCount": 5
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.MaxRetryCount.ShouldBe(5);
    }

    [Fact]
    public void MaxRetryCount_NotSet()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.MaxRetryCount.ShouldBe(0); // Default value
    }

    [Fact]
    public void InitialDelay_SetCorrectly()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "InitialDelay": "00:00:05"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.InitialDelay.ShouldBe(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public void InitialDelay_NotSet()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.InitialDelay.ShouldBe(TimeSpan.Zero); // Default value
    }

    [Fact]
    public void UseExponentialBackoff_SetCorrectly()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "UseExponentialBackoff": true
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.UseExponentialBackoff.ShouldBeTrue();
    }

    [Fact]
    public void UseExponentialBackoff_NotSet()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.UseExponentialBackoff.ShouldBeFalse(); // Default value
    }

    [Fact]
    public void BatchSize_SetCorrectly()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "BatchSize": 1000
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.BatchSize.ShouldBe(1000);
    }

    [Fact]
    public void BatchSize_NotSet()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.BatchSize.ShouldBe(0); // Default value
    }

    [Fact]
    public void NotifyAfter_SetCorrectly()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]",
                                    "NotifyAfter": 500
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.NotifyAfter.ShouldBe(500);
    }

    [Fact]
    public void NotifyAfter_NotSet()
    {
        // Arrange
        const string settings = """
                                {
                                  "SqlBulkCopier": {
                                    "DestinationTableName": "[dbo].[Customer]"
                                  }
                                }
                                """;
        var configuration = BuildJsonConfig(settings);

        // Act
        var bulkCopierBuilder = FixedLengthBulkCopierParser.Parse(configuration);

        // Assert
        using var connection = OpenConnection();
        var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
        bulkCopier.NotifyAfter.ShouldBe(0); // Default value
    }

    public class BuildBuilder
    {
        [Fact]
        public void DestinationTableName_NotExists()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {
                                        "HasHeader": true,
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var act = () => FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));

            // Assert
            act.ShouldThrow<InvalidOperationException>();
        }

        public class DefaultColumnContext
        {
            public class TrimChars
            {
                [Fact]
                public void NotExists()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimChars.ShouldBeNull();
                }

                [Fact]
                public void Trim()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "TrimMode": "Trim",
                                                  "TrimChars": "chars"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimChars.ShouldBeEquivalentTo("chars".ToCharArray());
                }

                [Fact]
                public void TrimStart()
                {
                    // Arrange
                    var settings =
                        """
                        {
                          "SqlBulkCopier": {
                            "DestinationTableName": "[dbo].[Customer]",
                            "HasHeader": true,
                            "DefaultColumnSettings": {
                              "TrimMode": "TrimStart",
                              "TrimChars": "chars"
                            }
                          }
                        }
                        """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimChars.ShouldBeEquivalentTo("chars".ToCharArray());
                }

                [Fact]
                public void TrimEnd()
                {
                    // Arrange
                    var settings =
                        """
                        {
                          "SqlBulkCopier": {
                            "DestinationTableName": "[dbo].[Customer]",
                            "HasHeader": true,
                            "DefaultColumnSettings": {
                              "TrimMode": "TrimEnd",
                              "TrimChars": "chars"
                            }
                          }
                        }
                        """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimChars.ShouldBeEquivalentTo("chars".ToCharArray());
                }
            }

            public class TrimMode
            {
                [Fact]
                public void NotExists()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimMode.ShouldBe(SqlBulkCopier.TrimMode.None);
                }

                [Fact]
                public void None()
                {
                    // Arrange
                    var settings =
                        """
                        {
                          "SqlBulkCopier": {
                            "DestinationTableName": "[dbo].[Customer]",
                            "HasHeader": true,
                            "DefaultColumnSettings": {
                              "TrimMode": "None"
                            }
                          }
                        }
                        """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimMode.ShouldBe(SqlBulkCopier.TrimMode.None);
                }

                [Fact]
                public void Trim()
                {
                    // Arrange
                    var settings =
                        """
                        {
                          "SqlBulkCopier": {
                            "DestinationTableName": "[dbo].[Customer]",
                            "HasHeader": true,
                            "DefaultColumnSettings": {
                              "TrimMode": "Trim"
                            }
                          }
                        }
                        """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimMode.ShouldBe(SqlBulkCopier.TrimMode.Trim);
                }

                [Fact]
                public void TrimStart()
                {
                    // Arrange
                    var settings =
                        """
                        {
                          "SqlBulkCopier": {
                            "DestinationTableName": "[dbo].[Customer]",
                            "HasHeader": true,
                            "DefaultColumnSettings": {
                              "TrimMode": "TrimStart"
                            }
                          }
                        }
                        """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimMode.ShouldBe(SqlBulkCopier.TrimMode.TrimStart);
                }

                [Fact]
                public void TrimEnd()
                {
                    // Arrange
                    var settings =
                        """
                        {
                          "SqlBulkCopier": {
                            "DestinationTableName": "[dbo].[Customer]",
                            "HasHeader": true,
                            "DefaultColumnSettings": {
                              "TrimMode": "TrimEnd"
                            }
                          }
                        }
                        """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TrimMode.ShouldBe(SqlBulkCopier.TrimMode.TrimEnd);
                }
            }

            public class TreatEmptyStringAsNull
            {
                [Fact]
                public void True()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "TreatEmptyStringAsNull": true
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TreatEmptyStringAsNull.ShouldBeTrue();
                }

                [Fact]
                public void False()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "TreatEmptyStringAsNull": false
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TreatEmptyStringAsNull.ShouldBeFalse();
                }

                [Fact]
                public void NotExists()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.TreatEmptyStringAsNull.ShouldBeFalse();
                }
            }

            public class SqlDbType
            {
                [Fact]
                public void None()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "NumberStyles": "None"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void Char()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "Char"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void NChar()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "NChar"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void VarChar()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "VarChar"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void NVarChar()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "NVarChar"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void Text()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "Text"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void NText()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "NText"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void Xml()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "Xml"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBeNull();
                }

                [Fact]
                public void Binary()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "Binary"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBe(System.Data.SqlDbType.Binary);
                }

                [Fact]
                public void Bit()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "Bit"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBe(System.Data.SqlDbType.Bit);
                }

                [Fact]
                public void Image()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "Image"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBe(System.Data.SqlDbType.Image);
                }

                [Fact]
                public void UniqueIdentifier()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "UniqueIdentifier"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBe(System.Data.SqlDbType.UniqueIdentifier);
                }

                [Fact]
                public void VarBinary()
                {
                    // Arrange
                    const string settings = """
                                            {
                                              "SqlBulkCopier": {
                                                "DestinationTableName": "[dbo].[Customer]",
                                                "HasHeader": true,
                                                "DefaultColumnSettings": {
                                                  "SqlDbType": "VarBinary"
                                                }
                                              }
                                            }
                                            """;
                    var configuration = BuildJsonConfig(settings);

                    // Act
                    var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                    builder.DefaultColumnContext(context);
                    var column = context.Build(_ => { });

                    // Assert
                    column.SqlDbType.ShouldBe(System.Data.SqlDbType.VarBinary);
                }

                public class BigInt
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "BigInt"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.BigInt);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "BigInt",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.BigInt);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class Decimal
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Decimal"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Decimal);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Decimal",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Decimal);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class Float
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Float"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Float);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Float",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Float);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class Int
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Int"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Int);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Int",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Int);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class Money
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Money"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Money);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Money",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Money);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class Real
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Real"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Real);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Real",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Real);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class SmallInt
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "SmallInt"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.SmallInt);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "SmallInt",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.SmallInt);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class SmallMoney
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "SmallMoney"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.SmallMoney);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "SmallMoney",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.SmallMoney);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class TinyInt
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "TinyInt"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.TinyInt);
                        column.CultureInfo.ShouldBeNull();
                        column.NumberStyles.ShouldBe(NumberStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "TinyInt",
                                                      "CultureInfo": "en-US",
                                                      "NumberStyles": "Any"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.TinyInt);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.NumberStyles.ShouldBe(NumberStyles.Any);
                    }
                }

                public class Date
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Date"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Date);
                        column.CultureInfo.ShouldBeNull();
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Date",
                                                      "CultureInfo": "en-US",
                                                      "DateTimeStyles": "AllowWhiteSpaces"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Date);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.AllowWhiteSpaces);
                    }
                }

                public class DateTime
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "DateTime"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.DateTime);
                        column.CultureInfo.ShouldBeNull();
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "DateTime",
                                                      "CultureInfo": "en-US",
                                                      "DateTimeStyles": "AllowWhiteSpaces"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.DateTime);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.AllowWhiteSpaces);
                    }
                }

                public class DateTime2
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "DateTime2"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.DateTime2);
                        column.CultureInfo.ShouldBeNull();
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "DateTime2",
                                                      "CultureInfo": "en-US",
                                                      "DateTimeStyles": "AllowWhiteSpaces"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.DateTime2);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.AllowWhiteSpaces);
                    }
                }

                public class DateTimeOffset
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "DateTimeOffset"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.DateTimeOffset);
                        column.CultureInfo.ShouldBeNull();
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "DateTimeOffset",
                                                      "CultureInfo": "en-US",
                                                      "DateTimeStyles": "AllowWhiteSpaces"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.DateTimeOffset);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.AllowWhiteSpaces);
                    }
                }

                public class SmallDateTime
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "SmallDateTime"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.SmallDateTime);
                        column.CultureInfo.ShouldBeNull();
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "SmallDateTime",
                                                      "CultureInfo": "en-US",
                                                      "DateTimeStyles": "AllowWhiteSpaces"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.SmallDateTime);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.AllowWhiteSpaces);
                    }
                }

                public class Time
                {
                    [Fact]
                    public void NoParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Time"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Time);
                        column.CultureInfo.ShouldBeNull();
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.None);
                    }

                    [Fact]
                    public void WithParameter()
                    {
                        // Arrange
                        const string settings = """
                                                {
                                                  "SqlBulkCopier": {
                                                    "DestinationTableName": "[dbo].[Customer]",
                                                    "HasHeader": true,
                                                    "DefaultColumnSettings": {
                                                      "SqlDbType": "Time",
                                                      "CultureInfo": "en-US",
                                                      "DateTimeStyles": "AllowWhiteSpaces"
                                                    }
                                                  }
                                                }
                                                """;
                        var configuration = BuildJsonConfig(settings);

                        // Act
                        var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
                        builder.DefaultColumnContext(context);
                        var column = context.Build(_ => { });

                        // Assert
                        column.SqlDbType.ShouldBe(System.Data.SqlDbType.Time);
                        column.CultureInfo.ShouldBe(CultureInfo.GetCultureInfo("en-US"));
                        column.DateTimeStyle.ShouldBe(DateTimeStyles.AllowWhiteSpaces);
                    }
                }
            }
        }

        public class RowFilter
        {
            [Fact]
            public void StartsWith()

            {
                // Arrange
                const string settings = """
                                        {
                                          "SqlBulkCopier": {
                                            "DestinationTableName": "[dbo].[Customer]",
                                            "RowFilter": {
                                                "StartsWith": ["A", "B"]
                                            },
                                            "Columns": {
                                              "CustomerId": { "Offset": 0, "Length": 5 }
                                            }
                                          }
                                        }
                                        """;
                var configuration = BuildJsonConfig(settings);

                // Act
                var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));

                // Assert
                var encoding = new UTF8Encoding(false);
                using var stream = new MemoryStream(encoding.GetBytes(
                    """
                    A skip by row filter
                    12345A
                    B skip by row filter
                    """));
                using var fixedLengthReader = new FixedLengthReader(stream, encoding);
                FixedLengthDataReader reader = new(fixedLengthReader, [], builder.RowFilter);
                reader.Read().ShouldBeTrue();
                encoding.GetString(fixedLengthReader.CurrentRow).ShouldBe("12345A");
                reader.Read().ShouldBeFalse();

            }

            [Fact]
            public void WhenEquals()
            {
                // Arrange
                const string settings = """
                                        {
                                          "SqlBulkCopier": {
                                            "DestinationTableName": "[dbo].[Customer]",
                                            "RowFilter": {
                                                "Equals": ["A", "B"]
                                            },
                                            "Columns": {
                                              "CustomerId": { "Offset": 0, "Length": 5 }
                                            }
                                          }
                                        }
                                        """;
                var configuration = BuildJsonConfig(settings);

                // Act
                var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));

                // Assert
                var encoding = new UTF8Encoding(false);
                using var stream = new MemoryStream(encoding.GetBytes(
                    """
                    A
                    A12345B
                    B
                    """));
                using var fixedLengthReader = new FixedLengthReader(stream, encoding);
                FixedLengthDataReader reader = new(fixedLengthReader, [], builder.RowFilter);
                reader.Read().ShouldBeTrue();
                encoding.GetString(fixedLengthReader.CurrentRow).ShouldBe("A12345B");
                reader.Read().ShouldBeFalse();

            }

            [Fact]
            public void EndsWith()
            {
                // Arrange
                const string settings = """
                                        {
                                          "SqlBulkCopier": {
                                            "DestinationTableName": "[dbo].[Customer]",
                                            "RowFilter": {
                                                "EndsWith": ["A", "B"]
                                            },
                                            "Columns": {
                                              "CustomerId": { "Offset": 0, "Length": 5 }
                                            }
                                          }
                                        }
                                        """;
                var configuration = BuildJsonConfig(settings);

                // Act
                var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));

                // Assert
                var encoding = new UTF8Encoding(false);
                using var stream = new MemoryStream(encoding.GetBytes(
                    """
                    skip by row filter A
                    12345
                    skip by row filter B
                    """));
                using var fixedLengthReader = new FixedLengthReader(stream, encoding);
                FixedLengthDataReader reader = new(fixedLengthReader, [], builder.RowFilter);
                reader.Read().ShouldBeTrue();
                encoding.GetString(fixedLengthReader.CurrentRow).ShouldBe("12345");
                reader.Read().ShouldBeFalse();

            }
        }

        [Fact]
        public void Column()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {
                                        "DestinationTableName": "[dbo].[Customer]",
                                        "HasHeader": true,
                                        "Columns": {
                                          "CustomerId": { "Offset": 0, "Length": 10 },
                                          "BirthDate": {
                                            "Offset": 10, 
                                            "Length": 8,
                                            "SqlDbType": "Date",
                                            "Format": "yyyyMMdd"
                                          }
                                        }
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var builder = (FixedLengthBulkCopierBuilder)FixedLengthBulkCopierParser.BuildBuilder(configuration.GetSection("SqlBulkCopier"));
            var context = new FixedLengthColumnContext(0, string.Empty, 1, 2);
            builder.DefaultColumnContext(context);
            context.Build(_ => { });

            // Assert
            builder.ColumnContexts.Count.ShouldBe(2);
            var customerId = (FixedLengthColumnContext)builder.ColumnContexts.Single(x => x.Name == "CustomerId");
            customerId.ShouldNotBeNull();
            customerId.OffsetBytes.ShouldBe(0);
            customerId.LengthBytes.ShouldBe(10);

            var birthDate = (FixedLengthColumnContext)builder.ColumnContexts.Single(x => x.Name == "BirthDate");
            birthDate.ShouldNotBeNull();
            birthDate.OffsetBytes.ShouldBe(10);
            birthDate.LengthBytes.ShouldBe(8);
            birthDate.SqlDbType.ShouldBe(SqlDbType.Date);
            birthDate.Format.ShouldBe("yyyyMMdd");
        }
    }

    static IConfiguration BuildJsonConfig(string json)
    {
        using var memoryStream = new MemoryStream(Encoding.UTF8.GetBytes(json));
        return new ConfigurationBuilder()
            .AddJsonStream(memoryStream)
            .Build();
    }
}