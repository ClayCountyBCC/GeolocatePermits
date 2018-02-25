using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeolocatePermits.Models
{
  public class Point
  {
    public double X { get; set; } = double.MinValue;
    public double Y { get; set; } = double.MinValue;
    public double Latitude { get; set; } = double.MinValue;
    public double Longitude { get; set; } = double.MinValue;
    public bool IsValid
    {
      get
      {
        return X != double.MinValue;
      }
    }
    public Point()
    {

    }
    public Point(double? NewX, double? NewY)
    {
      if (NewX.HasValue && NewY.HasValue)
      {
        X = NewX.Value;
        Y = NewY.Value;
        ToLatLong();
      }
    }
    private void ToLatLong()
    {
      string source_wkt = @"PROJCS[""NAD_1983_HARN_StatePlane_Florida_East_FIPS_0901_Feet"",GEOGCS[""GCS_North_American_1983_HARN"",DATUM[""NAD83_High_Accuracy_Regional_Network"",SPHEROID[""GRS_1980"",6378137.0,298.257222101]],PRIMEM[""Greenwich"",0.0],UNIT[""Degree"",0.0174532925199433]],PROJECTION[""Transverse_Mercator""],PARAMETER[""False_Easting"",656166.6666666665],PARAMETER[""False_Northing"",0.0],PARAMETER[""Central_Meridian"",-81.0],PARAMETER[""Scale_Factor"",0.9999411764705882],PARAMETER[""Latitude_Of_Origin"",24.33333333333333],UNIT[""Foot_US"",0.3048006096012192]]";
      var x = new ProjNet.CoordinateSystems.CoordinateSystemFactory();
      var csource = x.CreateFromWkt(source_wkt);
      var ctarget = ProjNet.CoordinateSystems.GeographicCoordinateSystem.WGS84;
      var t = new ProjNet.CoordinateSystems.Transformations.CoordinateTransformationFactory();
      var trans = t.CreateFromCoordinateSystems(csource, ctarget);
      double[] point = { X, Y };
      double[] convpoint = trans.MathTransform.Transform(point);
      Longitude = convpoint[0];
      Latitude = convpoint[1];
    }


  }
}
