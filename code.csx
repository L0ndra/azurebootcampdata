using CsvHelper;
using Microsoft.WindowsAzure.Storage.Table;

public static void Run(Stream myBlob, string name, TraceWriter log, IQueryable<TableModel> inputTable, ICollector<TableModel> outputTable, ICollector<string> outputQueueItem)
{
    using (var reader = new StreamReader(myBlob))
    {
        var csv = new CsvReader(reader);

        try
        {
            var records = csv.GetRecords<CsvModel>().ToList();
            foreach(var record in records)
            {
                var duplicate = inputTable.Where(x => x.PartitionKey == record.DeviceId && x.RowKey == record.EventId);
                if(duplicate.Count() > 0)
                {
                    outputQueueItem.Add($"Duplicate record for device {record.DeviceId} event {record.EventId} in file {name}");
                }
                else
                {
                    outputTable.Add(new TableModel{
                        PartitionKey = record.DeviceId,
                        RowKey = record.EventId,
                        Cost = record.Cost,
                        Duration = record.Duration
                    });
                }
            }
        }
        catch (Exception ex)
        {
            outputQueueItem.Add($"Failed to parse file {name} with exception {ex.ToString()}");
        }

    }
}

public class CsvModel
{
    public string DeviceId { get; set; }
    public string EventId { get; set; }
    public string Duration { get; set; }
    public string Cost { get; set; }
}

public class TableModel : TableEntity
{
    public string Duration {get;set;}
    public string Cost {get;set;}
}

#r "SendGrid"

using System;
using SendGrid.Helpers.Mail;

public static Mail Run(string myQueueItem, TraceWriter log)
{
    var mail = new Mail();
    mail.AddContent(new Content{
        Type = "text/plain",
        Value = myQueueItem
    });
    return mail;
}