// ReSharper disable UnusedMember.Global
namespace SqlBulkCopier.Test;

public class Customer
{
    public int? CustomerId { get; set; }
    public string? FirstName { get; set; }
    public string? LastName { get; set; }
    public string? Email { get; set; }
    public string? PhoneNumber { get; set; }
    public string? AddressLine1 { get; set; }
    public string? AddressLine2 { get; set; }
    public string? City { get; set; }
    public string? State { get; set; }
    public string? PostalCode { get; set; }
    public string? Country { get; set; }
    public DateTime? BirthDate { get; set; }
    public string? Gender { get; set; }
    public string? Occupation { get; set; }
    public decimal? Income { get; set; }
    public DateTime? RegistrationDate { get; set; }
    public DateTime? LastLogin { get; set; }
    public bool? IsActive { get; set; }
    public string? Notes { get; set; }

    // デフォルト値がテーブル上で設定されるので、Insert 時に必須ではないですが、
    // ここでは一応プロパティとして用意しておきます。
    public DateTime? CreatedAt { get; set; }
    public DateTime? UpdatedAt { get; set; }
}