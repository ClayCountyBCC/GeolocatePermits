using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using Dapper;
using GeolocatePermits.Models;
using System.Configuration;

namespace GeolocatePermits
{
  class Program
  {
    public const string WATSC = "WATSC";
    public const string GIS = "GIS";
    public const string LOG = "LOG";

    static void Main(string[] args)
    {
      var permits = BasePermit.Get();
      if (permits.Count == 0)  return;
      var addresses = (from p in permits
                       select p.LookupKey).Distinct().ToList();
      var parcels = (from p in permits
                     select p.ParcelNo).Distinct().ToList();
      var addressPoints = Point.GetAddressPoints(addresses);
      var parcelPoints = Point.GetParcelPoints(parcels);
      UpdatePermitData(ref permits, addressPoints, parcelPoints);      
      SavePermits(permits);
    }

    public static void UpdatePermitData(ref List<BasePermit> permits, 
      Dictionary<string, Point> addressPoints,
      Dictionary<string, Point> parcelPoints)
    {
      foreach(BasePermit p in permits)
      {
        if (addressPoints.ContainsKey(p.LookupKey) && 
            addressPoints[p.LookupKey].IsValid)
        {
          p.Project_Address_X = addressPoints[p.LookupKey].X;
          p.Project_Address_Y = addressPoints[p.LookupKey].Y;
        }
        else
        {
          // hrm, what to do if we didn't match that address?
          p.Project_Address_X = 0;
          p.Project_Address_Y = 0;
        }
        if (parcelPoints.ContainsKey(p.ParcelNo))
        {
          p.Parcel_Centroid_X = parcelPoints[p.ParcelNo].X;
          p.Parcel_Centroid_Y = parcelPoints[p.ParcelNo].Y;
        }
        else
        {
          p.Parcel_Centroid_X = 0;
          p.Parcel_Centroid_Y = 0;
        }

        // now let's update the X / Y with the 
        // address point if we've got it, otherwise the 
        // parcel point.
        if(p.Project_Address_X != 0)
        {
          p.X = Convert.ToInt32(p.Project_Address_X);
          p.Y = Convert.ToInt32(p.Project_Address_Y);
        }
        else
        {
          if (p.Parcel_Centroid_X != 0)
          {
            p.X = Convert.ToInt32(p.Parcel_Centroid_X);
            p.Y = Convert.ToInt32(p.Parcel_Centroid_Y);
          }
          else
          {
            p.X = 0;
            p.Y = 0;
          }
        }

      }
    }


    private static DataTable CreateDataTable()
    {
      var dt = new DataTable("GeoCoding");
      dt.Columns.Add("BaseID", typeof(int));
      dt.Columns.Add("X", typeof(double));
      dt.Columns.Add("Y", typeof(double));
      dt.Columns.Add("Project_Address_X", typeof(double));
      dt.Columns.Add("Project_Address_Y", typeof(double));
      dt.Columns.Add("Parcel_Centroid_X", typeof(double));
      dt.Columns.Add("Parcel_Centroid_Y", typeof(double));
      return dt;
    }

    public static void SavePermits(List<BasePermit> permits)
    {
      var dt = CreateDataTable();
      var changed = (from p in permits
                     where p.X != 0
                     select p);
      foreach(BasePermit p in changed)
      {
        dt.Rows.Add(p.BaseID, p.X, p.Y, p.Project_Address_X, 
          p.Project_Address_Y, p.Parcel_Centroid_X, p.Parcel_Centroid_Y);
      }
      string query = @"
        UPDATE B
        SET 
          B.X = G.X,
          B.Y = G.Y,
          B.Project_Address_X = G.Project_Address_X,
          B.Project_Address_Y = G.Project_Address_Y,
          B.Parcel_Centroid_X = G.Parcel_Centroid_X,
          B.Parcel_Centroid_Y = G.Parcel_Centroid_Y
        FROM bpBASE_PERMIT B
        INNER JOIN @GeoCoding G ON B.BaseID = G.BaseID";
      using (IDbConnection db = new SqlConnection(Get_ConnStr(WATSC)))
      {
        db.Execute(query, new { GeoCoding = dt.AsTableValuedParameter("GeoCoding") }, commandTimeout: 60);
      }
    }

    public static List<T> Get_Data<T>(string query, string cs)
    {
      try
      {
        using (IDbConnection db = new SqlConnection(Get_ConnStr(cs)))
        {
          return (List<T>)db.Query<T>(query);
        }
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, query);
        return null;
      }
    }

    public static List<T> Get_Data<T>(string query, DynamicParameters dbA, string cs, int timeOut = 60)
    {
      try
      {
        using (IDbConnection db =
          new SqlConnection(
            Get_ConnStr(cs)))
        {
          return (List<T>)db.Query<T>(query, dbA, commandTimeout: timeOut);
        }
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, query);
        return null;
      }
    }

    public static int Exec_Query(string query, DynamicParameters dbA, string cs)
    {
      try
      {
        using (IDbConnection db =
          new SqlConnection(
            Get_ConnStr(cs)))
        {
          return db.Execute(query, dbA);
        }
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, query);
        return -1;
      }
    }

    public static string Get_ConnStr(string cs)
    {
      return ConfigurationManager.ConnectionStrings[cs].ConnectionString;
    }

  }
}

