"use client";

import { useEffect } from "react";

/**
 * @param {JSON} forecasts - List of JSON object of forecasts ex:[{"day": "Sunday", "value": 760}]
 * @param {string} className - tailwind CSS for entire component styling
 * @returns Component displaying each day's numeric forecast
 */
export default function TopBar({ forecasts, className }) {
    // Function to scroll the top bar to today, if on phone
    useEffect(() => {
        if (typeof window !== "undefined" && window.innerWidth <= 768) {
          const today = new Date().getDay(); // 0 = Sunday
          const element = document.querySelector(`[data-day="${today}"]`);
          if (element) {
            element.scrollIntoView({ behavior: "smooth", inline: "center", block: "nearest" });
          }
        }
      }, []);

    return (
        <div className={className}>
            <div className="flex items-center justify-between w-[80vw] md:w-[90vw] py-4 md:p-4 bg-primary/40 text-white rounded-xl divide-x overflow-auto">
                {forecasts.map((item, index) => (
                    <div key={index} data-day={index} className="flex-1 px-4 text-center">
                        <div className="text-sm font-bold">{item.day.slice(0,3)}</div>
                        <div className="text-lg">{item.value / 10}%</div>
                    </div>
                ))}
            </div>
        </div>
    );
}