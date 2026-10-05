import { useEffect, useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import "./App.css";

function App() {
  const [metrics, setMetrics] = useState([]);


  useEffect(() => {
    const ws = new WebSocket("ws://127.0.0.1:8001/ws");
  
    ws.onopen = () => {
      console.log("StreamPulse WebSocket connected");
    };
  
    // ws.onmessage = (event) => {
    //   const data = JSON.parse(event.data);
    //   setMetrics(data);
    // };

    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
    
      console.log("Live metrics received:", data);
    
      setMetrics(data);
    };
  
    ws.onerror = (error) => {
      console.error("WebSocket error:", error);
    };
  
    ws.onclose = () => {
      console.log("StreamPulse WebSocket disconnected");
    };
  
    return () => {
      ws.close();
    };
  }, []);

  const totalLogs = metrics.reduce(
    (sum, item) => sum + item.total_logs,
    0
  );

  const totalErrors = metrics.reduce(
    (sum, item) => sum + item.error_count,
    0
  );

  const totalWarnings = metrics.reduce(
    (sum, item) => sum + item.warn_count,
    0
  );

  const avgResponseTime =
    metrics.length > 0
      ? (
          metrics.reduce(
            (sum, item) => sum + item.avg_response_time,
            0
          ) / metrics.length
        ).toFixed(2)
      : 0;

  const healthyServices = metrics.filter(
        (item) => item.status === "HEALTHY"
      ).length;
      
      const warningServices = metrics.filter(
        (item) => item.status === "WARNING"
      ).length;
      
      const criticalServices = metrics.filter(
        (item) => item.status === "CRITICAL"
      ).length;

  return (
    <div className="dashboard">
      <header>
        <h1>StreamPulse</h1>
        <p>Real-Time Distributed Log Analytics</p>

        <div className="live-status">
  <span className="live-dot"></span>
  LIVE · Updating every 5 seconds
</div>
      </header>

      <section className="cards">
        <div className="card">
          <h3>Total Logs</h3>
          <p>{totalLogs}</p>
        </div>

        <div className="card">
          <h3>Errors</h3>
          <p>{totalErrors}</p>
        </div>

        <div className="card">
          <h3>Warnings</h3>
          <p>{totalWarnings}</p>
        </div>

        <div className="card">
          <h3>Avg Response Time</h3>
          <p>{avgResponseTime} ms</p>
        </div>

        <div className="card health-card healthy-card">
  <h3>Healthy Services</h3>
  <p>{healthyServices}</p>
</div>

<div className="card health-card warning-card">
  <h3>Warning Services</h3>
  <p>{warningServices}</p>
</div>

<div className="card health-card critical-card">
  <h3>Critical Services</h3>
  <p>{criticalServices}</p>
</div>
      </section>

      <section className="services">
        <h2>Service Metrics</h2>

        <table>
          <thead>
            <tr>
              <th>Service</th>
              <th>Total Logs</th>
              <th>Errors</th>
              <th>Warnings</th>
              <th>Avg Response</th>
              <th>Max Response</th>
              <th>Status</th>
            </tr>
          </thead>

          <tbody>
            {metrics.map((item) => (
              <tr key={`${item.service}-${item.event_time}`}>
                <td>{item.service}</td>
                <td>{item.total_logs}</td>
                <td>{item.error_count}</td>
                <td>{item.warn_count}</td>
                <td>
                  {item.avg_response_time?.toFixed(2)} ms
                </td>
                <td>{item.max_response_time} ms</td>
                <td>
                  <span className={`status ${item.status.toLowerCase()}`}>
                    {item.status}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
      <section className="chart-section">
        <h2>Error Count by Service</h2>

        <ResponsiveContainer width="100%" height={350}>
          <BarChart data={metrics}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="service" />
            <YAxis />
            <Tooltip />
            <Bar dataKey="error_count" />
          </BarChart>
        </ResponsiveContainer>
      </section>

      <section className="chart-section">
  <h2>Average Response Time by Service</h2>

  <ResponsiveContainer width="100%" height={350}>
    <BarChart data={metrics}>
      <CartesianGrid strokeDasharray="3 3" />
      <XAxis dataKey="service" />
      <YAxis />
      <Tooltip />
      <Bar dataKey="avg_response_time" />
    </BarChart>
  </ResponsiveContainer>
</section>
    </div>
  );
}

export default App;