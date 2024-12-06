using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class Location
{
    public int Id { get; set; }

    public string ValuesLocation { get; set; } = null!;
}
