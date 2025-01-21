using Microsoft.EntityFrameworkCore;
using Sample.SetupSampleDatabase;

namespace Benchmark;

public class ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : DbContext(options)
{
    // ReSharper disable once UnusedMember.Global
    public DbSet<Customer> Customers { get; set; }
}