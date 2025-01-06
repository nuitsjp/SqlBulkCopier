using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Sample.SetupSampleDatabase;

namespace Benchmark;

public class ApplicationDbContext(DbContextOptions<ApplicationDbContext> options) : DbContext(options)
{
    public DbSet<Customer> Customers { get; set; }
}