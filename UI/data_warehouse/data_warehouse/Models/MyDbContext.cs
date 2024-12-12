using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;

namespace data_warehouse.Models;

public partial class MyDbContext : DbContext
{
    public MyDbContext()
    {
    }

    public MyDbContext(DbContextOptions<MyDbContext> options)
        : base(options)
    {
    }

    public virtual DbSet<ControlDataConfig> ControlDataConfigs { get; set; }

    public virtual DbSet<Country> Countries { get; set; }

    public virtual DbSet<DateDim> DateDims { get; set; }

    public virtual DbSet<FactTable> FactTables { get; set; }

    public virtual DbSet<Latesreport> Latesreports { get; set; }

    public virtual DbSet<Location> Locations { get; set; }

    public virtual DbSet<Log> Logs { get; set; }

    public virtual DbSet<Staging> Stagings { get; set; }

    public virtual DbSet<WeatherDescription> WeatherDescriptions { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see http://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseMySQL("Server=MYSQL9001.site4now.net\n;Database=db_aaffed_tuleep;User=aaffed_tuleep;Password=lethanhtu2803;");

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ControlDataConfig>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("control_data_config");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.CreateBy)
                .HasMaxLength(255)
                .HasColumnName("create_by");
            entity.Property(e => e.Decription)
                .HasColumnType("text")
                .HasColumnName("decription");
            entity.Property(e => e.EmailReport)
                .HasMaxLength(255)
                .HasColumnName("email_report");
            entity.Property(e => e.EmailSent)
                .HasMaxLength(255)
                .HasColumnName("email_sent");
            entity.Property(e => e.Location)
                .HasMaxLength(1000)
                .HasColumnName("location");
            entity.Property(e => e.Name)
                .HasMaxLength(1000)
                .HasColumnName("name");
            entity.Property(e => e.PassEmail)
                .HasMaxLength(255)
                .HasColumnName("pass_email");
            entity.Property(e => e.UrlMainWeb)
                .HasMaxLength(1000)
                .HasColumnName("url_main_web");
            entity.Property(e => e.UrlWeb)
                .HasMaxLength(1000)
                .HasColumnName("url_web");
        });

        modelBuilder.Entity<Country>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("country");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ValuesCountry)
                .HasMaxLength(250)
                .HasColumnName("values_country");
        });

        modelBuilder.Entity<DateDim>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("date_dim");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.DateValues)
                .HasColumnType("datetime")
                .HasColumnName("date_values");
            entity.Property(e => e.Day).HasColumnName("day");
            entity.Property(e => e.Month).HasColumnName("month");
            entity.Property(e => e.Year).HasColumnName("year");
        });

        modelBuilder.Entity<FactTable>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("fact_table");

            entity.HasIndex(e => e.CountryId, "FK_fact_table_country");

            entity.HasIndex(e => e.LocationId, "FK_fact_table_location");

            entity.HasIndex(e => e.WeatherId, "FK_fact_table_weather_description");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.CountryId).HasColumnName("country_id");
            entity.Property(e => e.DateId).HasColumnName("date_id");
            entity.Property(e => e.DeadTime)
                .HasColumnType("datetime")
                .HasColumnName("dead_time");
            entity.Property(e => e.DewPoint).HasColumnName("dew_point");
            entity.Property(e => e.Humidity).HasColumnName("humidity");
            entity.Property(e => e.LocationId).HasColumnName("location_id");
            entity.Property(e => e.Pressure).HasColumnName("pressure");
            entity.Property(e => e.ReportId).HasColumnName("report_id");
            entity.Property(e => e.Temperature).HasColumnName("temperature");
            entity.Property(e => e.Visibility).HasColumnName("visibility");
            entity.Property(e => e.WeatherId).HasColumnName("weather_id");
        });

        modelBuilder.Entity<Latesreport>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("latesreport");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.Time)
                .HasMaxLength(10)
                .HasColumnName("time");
        });

        modelBuilder.Entity<Location>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("location");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ValuesLocation)
                .HasMaxLength(250)
                .HasColumnName("values_location");
        });

        modelBuilder.Entity<Log>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("log");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.LogDate)
                .HasColumnType("datetime")
                .HasColumnName("log_date");
            entity.Property(e => e.Note)
                .HasMaxLength(255)
                .HasColumnName("note");
            entity.Property(e => e.Process)
                .HasMaxLength(255)
                .HasColumnName("process");
            entity.Property(e => e.Status)
                .HasMaxLength(255)
                .HasColumnName("status");
        });

        modelBuilder.Entity<Staging>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("staging");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.CurrentTime)
                .HasColumnType("datetime")
                .HasColumnName("currentTime");
            entity.Property(e => e.DeadTime)
                .HasColumnType("datetime")
                .HasColumnName("dead_time");
            entity.Property(e => e.DewPoint).HasColumnName("dew_point");
            entity.Property(e => e.Humidity).HasColumnName("humidity");
            entity.Property(e => e.LatestReport)
                .HasMaxLength(250)
                .HasColumnName("latestReport");
            entity.Property(e => e.Location)
                .HasMaxLength(250)
                .HasColumnName("location");
            entity.Property(e => e.Nation)
                .HasMaxLength(250)
                .HasColumnName("nation");
            entity.Property(e => e.Pressure).HasColumnName("pressure");
            entity.Property(e => e.Temperature).HasColumnName("temperature");
            entity.Property(e => e.Visibility).HasColumnName("visibility");
            entity.Property(e => e.WeatherStatus)
                .HasMaxLength(250)
                .HasColumnName("weather_status");
        });

        modelBuilder.Entity<WeatherDescription>(entity =>
        {
            entity.HasKey(e => e.Id).HasName("PRIMARY");

            entity.ToTable("weather_description");

            entity.Property(e => e.Id).HasColumnName("id");
            entity.Property(e => e.ValuesWeather)
                .HasMaxLength(250)
                .HasColumnName("values_weather");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
