﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using System.Data;
using System.Data.SqlClient;

namespace GeolocatePermits.Models
{
  public class ErrorLog
  {
    public int AppId { get; set; } = 20035;
    public string ApplicationName { get; set; } = "GeolocatePermits";
    public string ErrorText { get; set; }
    public string ErrorMessage { get; set; }
    public string ErrorStacktrace { get; set; }
    public string ErrorSource { get; set; }
    public string Query { get; set; }

    public ErrorLog(string text,
      string message,
      string stacktrace,
      string source,
      string errorQuery)
    {
      ErrorText = text;
      ErrorMessage = message;
      ErrorStacktrace = stacktrace;
      ErrorSource = source;
      Query = errorQuery;
      SaveLog();
    }

    public ErrorLog(Exception ex, string errorQuery = "")
    {
      ErrorText = ex.ToString();
      ErrorMessage = ex.Message;
      ErrorStacktrace = ex.StackTrace;
      ErrorSource = ex.Source;
      Query = errorQuery;
      SaveLog();
    }

    private void OutputToConsole()
    {
      Console.WriteLine(ErrorText);
      Console.WriteLine(ErrorMessage);
      Console.WriteLine(ErrorStacktrace);
      Console.WriteLine(ErrorSource);
      Console.WriteLine(Query);
    }

    private void SaveLog()
    {
      OutputToConsole();
      string sql = @"
          INSERT INTO ErrorData 
          (applicationName, errorText, errorMessage, 
          errorStacktrace, errorSource, query)  
          VALUES (@applicationName, @errorText, @errorMessage,
            @errorStacktrace, @errorSource, @query);";

      using (IDbConnection db = new SqlConnection(Program.Get_ConnStr(Program.LOG)))
      {
        db.Execute(sql, this);
      }
    }

    public static void SaveEmail(string to, string subject, string body)
    {
      string sql = @"
          INSERT INTO EmailList 
          (EmailTo, EmailSubject, EmailBody)  
          VALUES (@To, @Subject, @Body);";

      try
      {
        var dbArgs = new Dapper.DynamicParameters();
        dbArgs.Add("@To", to);
        dbArgs.Add("@Subject", subject);
        dbArgs.Add("@Body", body);


        using (IDbConnection db = new SqlConnection(Program.Get_ConnStr(Program.LOG)))
        {
          db.Execute(sql, dbArgs);
        }
      }
      catch (Exception ex)
      {
        new ErrorLog(ex, sql);
      }
    }

  }
}
