using System.Data;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Shouldly;
using SqlBulkCopier.CsvHelper;
using SqlBulkCopier.CsvHelper.Hosting;

namespace SqlBulkCopier.Test.CsvHelper.Hosting;

public class CsvBulkCopierParserTest
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

    public class Parse : CsvBulkCopierParserTest
    {
        [Fact]
        public void HasHeader_True()
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
            var bulkCopierBuilder = CsvBulkCopierParser.Parse(configuration);

            // Assert
            using var connection = OpenConnection();
            var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
            bulkCopier.DataReaderBuilder.ShouldBeOfType<CsvDataReaderBuilder>();
            var builder = (CsvDataReaderBuilder)bulkCopier.DataReaderBuilder;
            builder.HasHeader.ShouldBeTrue();
        }

        [Fact]
        public void HasHeader_False()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {
                                        "DestinationTableName": "[dbo].[Customer]",
                                        "HasHeader": false
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var bulkCopierBuilder = CsvBulkCopierParser.Parse(configuration);

            // Assert
            using var connection = OpenConnection();
            var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
            var builder = (CsvDataReaderBuilder)bulkCopier.DataReaderBuilder;
            builder.HasHeader.ShouldBeFalse();
        }

        [Fact]
        public void HasHeader_NotExists()
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
            var bulkCopierBuilder = CsvBulkCopierParser.Parse(configuration);

            // Assert
            using var connection = OpenConnection();
            var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
            var builder = (CsvDataReaderBuilder)bulkCopier.DataReaderBuilder;
            builder.HasHeader.ShouldBeFalse();
        }

        [Fact]
        public void TruncateBeforeBulkInsert_True()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {
                                        "DestinationTableName": "[dbo].[Customer]",
                                        "HasHeader": true,
                                        "TruncateBeforeBulkInsert": true
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var bulkCopierBuilder = CsvBulkCopierParser.Parse(configuration);

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
                                        "HasHeader": true,
                                        "TruncateBeforeBulkInsert": false
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var bulkCopierBuilder = CsvBulkCopierParser.Parse(configuration);

            // Assert
            using var connection = OpenConnection();
            var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
            bulkCopier.TruncateBeforeBulkInsert.ShouldBeFalse();
        }

        [Fact]
        public void TruncateBeforeBulkInsert_NotExists()
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
            var bulkCopierBuilder = CsvBulkCopierParser.Parse(configuration);

            // Assert
            using var connection = OpenConnection();
            var bulkCopier = (BulkCopier)bulkCopierBuilder.Build(connection);
            bulkCopier.TruncateBeforeBulkInsert.ShouldBeFalse();
        }
    }
    public class ParseHasHeaderBulkCopier
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
            Func<object?> act = () => CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));

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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                    var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                    var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                        var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
                        var context = new CsvColumnContext(0, string.Empty, _ => { });
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
                                          "CustomerId": {},
                                          "BirthDate": {
                                            "SqlDbType": "Date",
                                            "Format": "yyyyMMdd"
                                          }
                                        }
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseHasHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
            var context = new CsvColumnContext(0, string.Empty, _ => { });
            builder.DefaultColumnContext(context);
            context.Build(_ => { });

            // Assert
            builder.ColumnContexts.Count.ShouldBe(2);
            var customerId = builder.ColumnContexts.SingleOrDefault(x => x.Name == "CustomerId");
            customerId.ShouldNotBeNull();

            var birthDate = builder.ColumnContexts.SingleOrDefault(x => x.Name == "BirthDate");
            birthDate.ShouldNotBeNull();
            birthDate.SqlDbType.ShouldBe(SqlDbType.Date);
            birthDate.Format.ShouldBe("yyyyMMdd");
        }
    }

    public class ParseHasNotHeaderBulkCopier
    {
        [Fact]
        public void DestinationTableName_NotExists()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {}
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            Func<object?> act = () => CsvBulkCopierParser.ParseNoHeaderBuilder(configuration.GetSection("SqlBulkCopier"));

            // Assert
            act.ShouldThrow<InvalidOperationException>();
        }

        [Fact]
        public void Column()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {
                                        "DestinationTableName": "[dbo].[Customer]",
                                        "HasHeader": false,
                                        "Columns": {
                                          "CustomerId": {
                                            "Ordinal": 2
                                          },
                                          "BirthDate": {
                                            "Ordinal": 4,
                                            "SqlDbType": "Date",
                                            "Format": "yyyyMMdd"
                                          }
                                        }
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            var builder = (CsvBulkCopierBuilder)CsvBulkCopierParser.ParseNoHeaderBuilder(configuration.GetSection("SqlBulkCopier"));
            var context = new CsvColumnContext(0, string.Empty, _ => { });
            builder.DefaultColumnContext(context);
            context.Build(_ => { });

            // Assert
            builder.ColumnContexts.Count.ShouldBe(2);
            var customerId = builder.ColumnContexts.SingleOrDefault(x => x.Name == "CustomerId");
            customerId.ShouldNotBeNull();
            customerId.Ordinal.ShouldBe(2);

            var birthDate = builder.ColumnContexts.SingleOrDefault(x => x.Name == "BirthDate");
            birthDate.ShouldNotBeNull();
            birthDate.Ordinal.ShouldBe(4);
            birthDate.SqlDbType.ShouldBe(SqlDbType.Date);
            birthDate.Format.ShouldBe("yyyyMMdd");
        }

        [Fact]
        public void Column_NotExistOrdinal()
        {
            // Arrange
            const string settings = """
                                    {
                                      "SqlBulkCopier": {
                                        "DestinationTableName": "[dbo].[Customer]",
                                        "HasHeader": false,
                                        "Columns": {
                                          "CustomerId": {}
                                        }
                                      }
                                    }
                                    """;
            var configuration = BuildJsonConfig(settings);

            // Act
            Func<object?> act = () => CsvBulkCopierParser.ParseNoHeaderBuilder(configuration.GetSection("SqlBulkCopier"));

            // Assert
            act.ShouldThrow<InvalidOperationException>();
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