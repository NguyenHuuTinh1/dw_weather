using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class Country
{
    public int Id { get; set; }

    public string ValuesCountry { get; set; } = null!;
}
