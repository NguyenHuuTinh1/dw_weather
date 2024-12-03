using data_warehouse.Models;
using data_warehouse.Services;
using Microsoft.EntityFrameworkCore;

var builder = WebApplication.CreateBuilder(args);

// Thêm các d?ch v? khác
builder.Services.AddControllersWithViews();
builder.Services.AddSession();

builder.Services.AddDbContext<MyDbContext>(options =>
    options.UseMySQL(builder.Configuration.GetConnectionString("DefaultConnection")));
builder.Services.AddScoped<StagingService, StagingServiceImpl>();


var app = builder.Build();

app.UseStaticFiles();
app.UseSession();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller}/{action}"
);


app.Run();
