using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class DateDim
{
    public int Id { get; set; }

    public DateTime? DateValues { get; set; }

    public int? Day { get; set; }

    public int? Month { get; set; }

    public int? Year { get; set; }
}
