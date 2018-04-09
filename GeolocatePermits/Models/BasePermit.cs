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

        WITH PassedFinal AS (
          SELECT 
            PermitNo,
            COUNT(InspReqID) AS TotalFinalInspections
          FROM bpINS_REQUEST I
          INNER JOIN bpINS_REF IR ON I.InspectionCode = IR.InspCd AND Final = 1 AND IR.Type <> 1
          WHERE ResultADC IN ('A', 'P')
          GROUP BY I.PermitNo
        ), BasePermitData AS (
          SELECT DISTINCT
            ISNULL(ProjAddrNumber, '') + '-' + 
              CASE WHEN LEN(LTRIM(RTRIM(ProjPreDir))) > 0 THEN '-' + LTRIM(RTRIM(ProjPreDir)) ELSE '' END + 
              ISNULL(ProjStreet, '') + '-' + 
              CASE WHEN LEN(LTRIM(RTRIM(ProjPostDir))) > 0 THEN '-' + LTRIM(RTRIM(ProjPostDir)) ELSE '' END + 
              ISNULL(ProjCity, '') + 
              CASE WHEN LEN(LTRIM(RTRIM(ISNULL(ProjZip, '')))) > 0 THEN ProjZip ELSE '99999' END LookupKey,
            BaseID,
            ParcelNo,
            X, 
            Y,
            Project_Address_X,
            Project_Address_Y,
            Parcel_Centroid_X,
            Parcel_Centroid_Y,
            Date_Geocoding_Updated,
            Geocoded_Parcel,
            Geocoded_Address
          FROM bpBASE_PERMIT
        )

        SELECT DISTINCT TOP 1000
          LookupKey,
          B.BaseID,
          ParcelNo,
          X,
          Y,
          Project_Address_X,
          Project_Address_Y,
          Parcel_Centroid_X,
          Parcel_Centroid_Y,
          Date_Geocoding_Updated,
          Geocoded_Parcel,
          Geocoded_Address
        FROM BasePermitData B
        LEFT OUTER JOIN bpMASTER_PERMIT M ON B.BaseID = M.BaseId
        LEFT OUTER JOIN bpASSOC_PERMIT A ON B.BaseID = A.BaseID
        LEFT OUTER JOIN PassedFinal F ON A.PermitNo = F.PermitNo
        WHERE          
          YEAR(ISNULL(M.IssueDate, A.IssueDate)) > 2014
          AND F.TotalFinalInspections IS NULL
          AND M.CoDate IS NULL
          AND M.VoidDate IS NULL 
          AND A.VoidDate IS NULL
          AND LEN(LTRIM(RTRIM(ParcelNo))) > 0
          AND (Date_Geocoding_Updated IS NULL
          OR B.ParcelNo != ISNULL(B.Geocoded_Parcel, '')
          OR B.LookupKey != ISNULL(B.Geocoded_Address, ''))";
      return Program.Get_Data<BasePermit>(query, Program.WATSC);
    }



  }
}
