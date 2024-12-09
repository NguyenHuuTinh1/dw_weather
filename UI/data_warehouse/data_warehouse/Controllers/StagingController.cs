using data_warehouse.Services;
using Microsoft.AspNetCore.Mvc;

namespace data_warehouse.Controllers
{
    [Route("staging")]
    public class StagingController : Controller
    {
        public StagingService stagingService;
        public StagingController(StagingService _stagingService) {
            stagingService = _stagingService;
        }
        [Route("index")]
        [Route("")]
        [Route("~/")]
        public IActionResult Index()
        {
            ViewBag.Staging = stagingService.findAll();
            return View();
        }
        [Route("index2")]
       
        public IActionResult Index2()
        {
            ViewBag.Staging = stagingService.findAll();
            return View("index2");
        }
    }
}
