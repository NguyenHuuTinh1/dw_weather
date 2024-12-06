using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class Log
{
    public int Id { get; set; }

    public string? Status { get; set; }

    public string? Note { get; set; }

    public DateTime? LogDate { get; set; }
}
