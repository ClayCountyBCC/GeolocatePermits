using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;


namespace GeolocatePermits.Models
{
  public class Point
  {
    public string LookupKey { get; set; } = ""; // this is either the address or the parcel number.
    public double X { get; set; } = double.MinValue;
    public double Y { get; set; } = double.MinValue;
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

    public static Dictionary<string, Point> GetAddressPoints(List<string> LookupKeys)
    {
      var dp = new DynamicParameters();
      dp.Add("@Keys", LookupKeys);
      string query = @"
          SELECT
            OBJECTID,
            CAST(House AS VARCHAR(50)) + '-' +
              CASE WHEN LEN(Unit) > 0 THEN '-' + LTRIM(RTRIM(Unit)) ELSE '' END  + 
              CASE WHEN LEN(PreDir) > 0 THEN '-' + LTRIM(RTRIM(PreDir)) ELSE '' END + 
              StreetName + '-' + 
              CASE WHEN LEN(SuffixDir) > 0 THEN '-' + LTRIM(RTRIM(SuffixDir)) ELSE '' END + 
              CAST(Community AS VARCHAR(50)) +
              CAST(Zip AS VARCHAR(50)) LookupKey,
              XCoord X,
              YCoord Y
          FROM ADDRESS_SITE
          WHERE 
            CAST(House AS VARCHAR(50)) + '-' +
              CASE WHEN LEN(Unit) > 0 THEN '-' + LTRIM(RTRIM(Unit)) ELSE '' END  + 
              CASE WHEN LEN(PreDir) > 0 THEN '-' + LTRIM(RTRIM(PreDir)) ELSE '' END + 
              StreetName + '-' + 
              CASE WHEN LEN(SuffixDir) > 0 THEN '-' + LTRIM(RTRIM(SuffixDir)) ELSE '' END + 
              CAST(Community AS VARCHAR(50)) +
              CAST(Zip AS VARCHAR(50)) IN @Keys
          ORDER BY OBJECTID DESC";
      try
      {
        var addressPoints = Program.Get_Data<Point>(query, dp, Program.GIS);
        var d = new Dictionary<string, Point>();
        foreach (Point p in addressPoints)
        {
          if (!d.ContainsKey(p.LookupKey) && p.IsValid)
          {
            d.Add(p.LookupKey, p);
          }
        }
        return d;
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, query);
        return null;
      }
    }

    public static Dictionary<string, Point> GetParcelPoints(List<string> LookupKeys)
    {
      var dp = new DynamicParameters();
      dp.Add("@Keys", LookupKeys);
      string query = @"
        SELECT 
          PIN LookupKey,
          geometry::UnionAggregate(SHAPE).STCentroid().STX X,
          geometry::UnionAggregate(SHAPE).STCentroid().STY Y
        FROM Clay.dbo.PARCEL_INFO P
        WHERE PIN IN @Keys
        GROUP BY P.PIN
        ORDER BY P.PIN";
      try
      {
        var parcelPoints = Program.Get_Data<Point>(query, dp, Program.GIS);
        var d = new Dictionary<string, Point>();
        foreach (Point p in parcelPoints)
        {
          if (!d.ContainsKey(p.LookupKey) && p.IsValid)
          {
            d.Add(p.LookupKey, p);
          }
        }
        return d;
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, query);
        return null;
      }

    }

  }
}
