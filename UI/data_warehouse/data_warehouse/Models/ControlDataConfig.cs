using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class ControlDataConfig
{
    public int Id { get; set; }

    public string? Name { get; set; }

    public string? Decription { get; set; }

    public string? UrlMainWeb { get; set; }

    public string? UrlWeb { get; set; }

    public string? Location { get; set; }

    public string? CreateBy { get; set; }

    public string? EmailReport { get; set; }

    public string? PassEmail { get; set; }

    public string? EmailSent { get; set; }
}
