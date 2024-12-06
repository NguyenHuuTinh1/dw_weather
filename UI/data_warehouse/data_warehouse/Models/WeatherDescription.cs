using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class WeatherDescription
{
    public int Id { get; set; }

    public string ValuesWeather { get; set; } = null!;
}
