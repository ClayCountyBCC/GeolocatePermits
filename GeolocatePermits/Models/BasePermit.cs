using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeolocatePermits.Models
{
  public class BasePermit
  {
    public int BaseID { get; set; }
    public string ParcelNo { get; set; }
    public string LookupKey { get; set; }
    public int X { get; set; } = 0;
    public int Y { get; set; } = 0;
    public double Project_Address_X { get; set; } = 0;
    public double Project_Address_Y { get; set; } = 0;
    public double Parcel_Centroid_X { get; set; } = 0;
    public double Parcel_Centroid_Y { get; set; } = 0;

    public BasePermit()
    {

    }

    public static List<BasePermit> Get()
    {
      string query = @"
        USE WATSC;
        SELECT DISTINCT TOP 1000
          ISNULL(ProjAddrNumber, '') + '-' + 
            CASE WHEN LEN(LTRIM(RTRIM(ProjPreDir))) > 0 THEN '-' + LTRIM(RTRIM(ProjPreDir)) ELSE '' END + 
            ISNULL(ProjStreet, '') + '-' + 
            CASE WHEN LEN(LTRIM(RTRIM(ProjPostDir))) > 0 THEN '-' + LTRIM(RTRIM(ProjPostDir)) ELSE '' END + 
            ISNULL(ProjCity, '') + 
            CASE WHEN LEN(LTRIM(RTRIM(ISNULL(ProjZip, '')))) > 0 THEN ProjZip ELSE '99999' END LookupKey,
          B.BaseID,
          ParcelNo,
          X, 
          Y,
          Project_Address_X,
          Project_Address_Y,
          Parcel_Centroid_X,
          Parcel_Centroid_Y
        FROM bpBASE_PERMIT B
        LEFT OUTER JOIN bpMASTER_PERMIT M ON B.BaseID = M.BaseId
        LEFT OUTER JOIN bpASSOC_PERMIT A ON B.BaseID = A.BaseID
        WHERE 
          ISNULL(M.PermitNo, A.PermitNo) IS NOT NULL
          AND Date_Row_Created IS NOT NULL
          AND Project_Address_X != 0";
      return Program.Get_Data<BasePermit>(query, Program.WATSC);
    }



  }
}
