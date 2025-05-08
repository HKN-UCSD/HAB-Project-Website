"use client";

import { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Label,
} from "recharts";

export default function Graph({ forecasts, className }) {
  // const [data, setData] = useState([]);

//   useEffect(() => {
//     fetch("/dummy_data.json")
//       .then((res) => res.json())
//       .then((json) => setData(json));
//   }, []);

  return (
    <div className={className}>
      <div
        className="w-[80vw] md:w-[50vw] p-6 rounded-xl bg-primary/40"
      >
        <h3 className="text-white text-lg mb-4">7 - Day Predictions</h3>
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={forecasts} margin={{ top: 20, right: 40, bottom: 40, left: 20 }}>
            <CartesianGrid stroke="rgba(255,255,255,0.2)" />
            <XAxis dataKey="day" tick={{ fill: "white" }}>
              <Label
                value="Day of the Week"
                position="insideBottom"
                offset={-30}
                style={{ fill: "white", textAnchor: "middle" }}
              />
            </XAxis>
            <YAxis tick={{ fill: "white" }}>
              <Label
                value="Models Predicted Bloom"
                angle={-90}
                position="insideLeft"
                offset={-10}
                style={{ fill: "white", textAnchor: "middle" }}
              />
            </YAxis>
            <Tooltip
              contentStyle={{ backgroundColor: "#1e3a47", borderColor: "var(--color-secondary)" }}
              labelStyle={{ color: "white" }}
              itemStyle={{ color: "var(--color-secondary)" }}
            />
            <Line
              type="monotone"
              dataKey="value"
              stroke="var(--color-secondary)"
              strokeWidth={2}
              dot={{
                r: 5,
                stroke: "var(--color-secondary)",
                strokeWidth: 2,
                fill: "#ffffff",
              }}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}