using data_warehouse.Models;

namespace data_warehouse.Services
{
    public class StagingServiceImpl : StagingService
    {
        public MyDbContext db;
        public StagingServiceImpl(MyDbContext _db) { 
            db = _db;
        }
        public dynamic findAll()
        {
            return db.Stagings.ToList();
        }
    }
}
