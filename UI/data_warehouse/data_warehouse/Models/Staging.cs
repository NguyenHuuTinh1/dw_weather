using System;
using System.Collections.Generic;

namespace data_warehouse.Models;

public partial class Staging
{
    public int Id { get; set; }

    public string Nation { get; set; } = null!;

    public double Temperature { get; set; }

    public string WeatherStatus { get; set; } = null!;

    public string Location { get; set; } = null!;

    public DateTime CurrentTime { get; set; }

    public string LatestReport { get; set; } = null!;

    public double Visibility { get; set; }

    public double Pressure { get; set; }

    public int Humidity { get; set; }

    public double DewPoint { get; set; }

    public DateTime DeadTime { get; set; }
}
