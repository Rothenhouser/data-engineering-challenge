# Hivemind Data Engineering Challenge

Consider a company called "EV Coorp." in the fleet management sector that operates EV charging infrastructure and is encountering several challenges related to data processing and analytics. These challenges include difficulties in understanding the operational status of their infrastructure, which is compounded by manual analysis processes that are both time-consuming and prone to errors. Additionally, they are facing issues with growing data volumes that their current systems are unable to manage effectively. There is also a pressing need for both real-time monitoring and historical insights, which their current setup does not adequately provide. Same applies for the absence of ad hoc analysis capabilities. Furthermore, the data they collect is fragmented over different data sources and their current solution fails to deliver clear business value on time.

As a consulting company, our role is to assist them in modernizing their data capabilities. This modernization will enable better decision-making and enhance operational efficiency.

In your role, you will be joining our team to assist in the design and construction of a data processing solution. Your responsibilities include analyzing the available data to determine the insights that can be extracted. You are also tasked with designing a system architecture that is meeting their requirements and allows easy extraction of fields that can be used build the insights you identified in the data earlier. Finally, you need to build a working prototype to demonstrate the value we can deliver based on the data we got from them. This is where all pieces come together.

The sample data we got from their charging infrastructure includes operational messages following the [OCPP protocol](https://openchargealliance.org/protocols/open-charge-point-protocol/#OCPP1.6):
- Infrastructure status and health indicators
- Session lifecycle events
- Real-time operational metrics
- System diagnostics and notifications

Because of the size the data, it is split into two files:
- [`ocpp-data-many-chargers.txt`](ocpp-data-many-chargers.txt)
- [`ocpp-data-many-days.txt`](ocpp-data-many-days.txt)

This dataset represents typical patterns you'd encounter in EV charging infrastructure between charge points and the grid. You find in the [`ocpp-data-many-chargers.txt`](ocpp-data-many-chargers.txt) file, here is an extract of the first rows:
```
charger6 : [2,"ef51a638-0e05-4a9d-be52-594ada28f153","MeterValues",{"connectorId":1,"transactionId":1,"meterValue":[{"timestamp":"2025-08-26T23:59:57.599Z","sampledValue":[{"value":"37.3","context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","location":"Outlet","unit":"A"},{"value":"30850.8733","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Register","location":"Outlet","unit":"kWh"},{"value":"0.0469","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Interval","location":"Outlet","unit":"kWh"},{"value":"20.7","context":"Sample.Periodic","format":"Raw","measurand":"Power.Active.Import","location":"Outlet","unit":"kW"},{"value":"587.2","context":"Sample.Periodic","format":"Raw","measurand":"Voltage","location":"Outlet","unit":"V"},{"value":"67","context":"Sample.Periodic","format":"Raw","measurand":"SoC","location":"EV","unit":"Percent"},{"value":"200","context":"Sample.Periodic","format":"Raw","measurand":"Current.Offered","location":"Outlet","unit":"A"},{"value":"60000","context":"Sample.Periodic","format":"Raw","measurand":"Power.Offered","location":"Outlet","unit":"W"}]}]}]
charger6 : [3,"ef51a638-0e05-4a9d-be52-594ada28f153",{}]
charger10 : [2,"2e71389f-174a-44ef-a11d-6fa4db1b75a2","MeterValues",{"connectorId":1,"transactionId":1,"meterValue":[{"timestamp":"2025-08-26T23:59:59.865Z","sampledValue":[{"value":"198.5","context":"Sample.Periodic","format":"Raw","measurand":"Current.Import","location":"Outlet","unit":"A"},{"value":"13135.6161","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Register","location":"Outlet","unit":"kWh"},{"value":"0.3119","context":"Sample.Periodic","format":"Raw","measurand":"Energy.Active.Import.Interval","location":"Outlet","unit":"kWh"},{"value":"124.8","context":"Sample.Periodic","format":"Raw","measurand":"Power.Active.Import","location":"Outlet","unit":"kW"},{"value":"628.6","context":"Sample.Periodic","format":"Raw","measurand":"Voltage","location":"Outlet","unit":"V"},{"value":"39","context":"Sample.Periodic","format":"Raw","measurand":"SoC","location":"EV","unit":"Percent"},{"value":"200","context":"Sample.Periodic","format":"Raw","measurand":"Current.Offered","location":"Outlet","unit":"A"},{"value":"150000","context":"Sample.Periodic","format":"Raw","measurand":"Power.Offered","location":"Outlet","unit":"W"}]}]}]
charger10 : [3,"2e71389f-174a-44ef-a11d-6fa4db1b75a2",{}]
charger8 : [2,"b5460d35-4848-4ae8-a9a1-4a7424db277c","Heartbeat",{ }]
charger8 : [3,"b5460d35-4848-4ae8-a9a1-4a7424db277c",{"currentTime":"2025-08-27T00:00:03.233944Z"}]
charger3 : [2,"6ae647ad-6a4c-430f-afd4-f3281c396dcb","Heartbeat",{ }]
charger3 : [3,"6ae647ad-6a4c-430f-afd4-f3281c396dcb",{"currentTime":"2025-08-27T00:00:04.186104Z"}]
charger6 : [2,"5621ea49-3c69-4f20-a0f3-4c56c45fce34","StatusNotification",{"connectorId":2,"errorCode":"NoError","info":"","status":"Available","timestamp":"2025-08-27T00:00:03.015Z","vendorId":"charger brand Technology","vendorErrorCode":""}]
```

# Deliverables

The success of this project will be determined by several key criteria.

1. **Working Solution**: A data processing system that transforms raw operational data into actionable insights
2. **Architecture Documentation**: Clear explanation of your design choices and how the system addresses the client's needs
3. **Demonstration**: A way to actually showcase the solution's capabilities and validate that it works as intended

While the solution doesn't need to be fully production-ready, document any shortcuts, technical debt, or production concerns that need addressing before deployment. Even though it might not required to be production ready right now, we still expect a portable deployment path that supports both the local development and can be adapted for future production deployment, or deployments to different environments. Make sure to take this into acccount from the architecture design perspective.

The solution should effectively process the data rows found in the `ocpp-data-*.txt` files, offering accessible methods for querying and inspecting the processed data. It should enable data aggregation across various dimensions such as time and chargers and potentially multiple sites in the future.

Consider this potential output structure:
```json
{
  "sessionId": "charger6_2025-08-26T23:59:57.599Z",
  "stationId": "charger6", 
  "status": "active",
  "startTime": "2025-08-26T23:59:57.599Z",
  "endTime": null,
  "duration": 457,
  "totalEnergyConsumed": {"value": 12.5, "unit": "kWh"},
  "eventCount": 23
}
```
The structure above is just one example - you're free to use any output format that serves the analytical needs: JSON, CSV, Parquet, Delta Tables, Iceberg, database tables, dashboards, or any other format that demonstrates your solution effectively. The output fields are also just an example. Feel free to add more or skip any if you think it's not relevant, maybe termination reason?

# Notes
- Choose technologies and approaches that best fit the problem - we value thoughtful technical decisions and a good balance between simplicity and performance and pragmatism
- Consider how to determine operational status in incomplete or inconsistent scenarios
- To calculate the energy consumption you would have to calculate the integral of the power curve over time, but for simplicity you can safely approximate this by the average of the power curve and multiply it by the duration of the session to create a simple estimate. e.g. 5kW * 1h = 5kWh. Look for `Power.Active.Import`.
