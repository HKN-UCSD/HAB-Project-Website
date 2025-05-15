"use client";

import { useEffect, useState } from 'react';
import {
    Bar,
    XAxis,
    BarChart,
    LabelList,
    ResponsiveContainer,
  } from "recharts";

/**
 * Droplet Icon
 */
function Droplet({ predValue, className }) {
    const [scale, setScale] = useState(0);
    const target = predValue / 1000;

    // Animate on mount
    useEffect(() => {
        setScale(1.05);

        const timeout = setTimeout(() => {
            setScale(target);
        }, 800);

        return () => clearTimeout(timeout);
    }, [target]);

    const centerX = 512;
    const centerY = 512;
    const transform = `
        translate(${centerX}, ${centerY})
        scale(${scale}, ${scale})
        translate(-${centerX}, -${centerY})
    `;
  
    return (
        <svg
            viewBox="0 0 1024 1024"
            className={className}
            xmlns="http://www.w3.org/2000/svg"
        >
            <g transform={transform} style={{ transition: 'transform 1s ease-in-out' }}>
                <path fill="var(--color-secondary)" opacity="1.000000" stroke="none" 
                    d="
                    M710.031616,741.991211 
                    C670.621155,792.969360 620.044678,825.861755 556.718384,836.866699 
                    C502.634949,846.265442 450.118530,839.570618 400.974152,814.050232 
                    C352.807434,789.037598 316.552399,751.856201 291.708832,703.582031 
                    C271.899597,665.090149 263.071838,624.172119 265.881470,581.023804 
                    C268.024078,548.119324 277.559235,516.989136 291.255249,487.111603 
                    C307.841064,450.930023 329.450714,417.682739 351.712708,384.868256 
                    C382.856232,338.962219 415.085205,293.812195 447.449646,248.759796 
                    C466.012299,222.919937 484.428772,196.974762 502.847015,171.031570 
                    C509.652985,161.444916 518.274170,161.242111 525.007935,170.848022 
                    C573.583252,240.142334 623.992859,308.144409 671.438416,378.231293 
                    C693.470886,410.777832 715.596619,443.297089 732.667358,478.894379 
                    C746.138855,506.986176 756.405518,536.125793 759.987122,567.151917 
                    C763.330444,596.113831 761.517090,624.896179 754.057922,653.253906 
                    C745.504456,685.772217 730.585938,715.064331 710.031616,741.991211 
                    M294.485382,568.964722 
                    z"
                />
            </g>
            {/* Outer Droplet Outline */}
            <path fill="var(--color-primary)" opacity="1.000000" stroke="var(--color-primary)" 
                d="
                M710.031616,741.991211 
                C670.621155,792.969360 620.044678,825.861755 556.718384,836.866699 
                C502.634949,846.265442 450.118530,839.570618 400.974152,814.050232 
                C352.807434,789.037598 316.552399,751.856201 291.708832,703.582031 
                C271.899597,665.090149 263.071838,624.172119 265.881470,581.023804 
                C268.024078,548.119324 277.559235,516.989136 291.255249,487.111603 
                C307.841064,450.930023 329.450714,417.682739 351.712708,384.868256 
                C382.856232,338.962219 415.085205,293.812195 447.449646,248.759796 
                C466.012299,222.919937 484.428772,196.974762 502.847015,171.031570 
                C509.652985,161.444916 518.274170,161.242111 525.007935,170.848022 
                C573.583252,240.142334 623.992859,308.144409 671.438416,378.231293 
                C693.470886,410.777832 715.596619,443.297089 732.667358,478.894379 
                C746.138855,506.986176 756.405518,536.125793 759.987122,567.151917 
                C763.330444,596.113831 761.517090,624.896179 754.057922,653.253906 
                C745.504456,685.772217 730.585938,715.064331 710.031616,741.991211 
                M294.485382,568.964722 
                C292.526581,588.738037 292.271820,608.525574 295.589813,628.152100 
                C300.616455,657.885315 311.283630,685.564026 327.641418,710.920349 
                C355.986969,754.858948 394.723328,785.826782 444.228821,802.902527 
                C472.939972,812.805725 502.537384,815.510437 532.778076,812.882996 
                C551.642761,811.243958 569.966003,807.424744 587.598694,800.713501 
                C636.097778,782.254272 674.436707,751.135620 701.542664,706.832031 
                C731.416016,658.005493 741.437927,605.431274 728.686157,549.214539 
                C720.930664,515.023926 705.108398,484.211914 687.152954,454.455444 
                C657.089600,404.633087 622.691406,357.728668 589.287781,310.166016 
                C568.036499,279.906799 546.256470,250.018784 524.695984,219.976868 
                C521.236389,215.156311 517.684692,210.401764 514.352234,205.857605 
                C512.142822,206.389603 511.635529,207.741684 510.882050,208.783737 
                C499.953430,223.897552 489.009277,239.000488 478.155914,254.168304 
                C456.935699,283.823975 435.668671,313.446930 414.607452,343.215363 
                C388.541046,380.058258 362.104767,416.667023 339.153717,455.595978 
                C318.431793,490.744019 300.179260,526.910645 294.485382,568.964722 
                z"
            />
            <path fill="var(--color-primary)" opacity="1.000000" stroke="var(--color-primary)" 
                d="
                M342.814545,575.407043 
                C352.813293,573.954224 359.469421,579.482422 359.208191,589.472961 
                C358.790131,605.461487 360.749664,621.052734 366.232849,636.042664 
                C382.740356,681.170837 412.427307,713.655029 458.004730,730.612183 
                C461.573334,731.939880 465.334564,732.778259 469.043030,733.695496 
                C476.942535,735.649170 480.998199,741.347717 479.497284,748.547668 
                C477.888489,756.264893 471.505768,760.401672 463.493347,758.884827 
                C438.867523,754.223083 417.260651,743.146484 398.084473,727.331299 
                C363.334259,698.671997 341.861053,662.122803 334.030762,617.683167 
                C332.199829,607.292053 332.382294,596.778015 332.829742,586.310608 
                C333.078796,580.484680 336.987701,577.134827 342.814545,575.407043 
                z"
            />
        </svg>
    );
}

/**
 * @param {JSON} forecasts - List of JSON object of forecasts ex:[{"day": "Sunday", "value": 760}]
 * @param {string} className - tailwind CSS for entire component styling
 * @returns Component displaying today's forecast
 */
export default function Today({forecasts, className}) {
    let date = new Date();
    let forecast = forecasts[date.getDay()];
    const TOTAL_MODELS = 1000;
    let chartData = [
        {'prediction': "Yes", 'value': forecast["value"]},
        {'prediction': "No", 'value': TOTAL_MODELS - forecast["value"]},
    ]
    let dayOptions = { weekday: 'long' };
    return (
        <div className={className}>
            <div className="relative flex flex-row w-[80vw] md:w-[40vw] items-center bg-primary/40 text-white rounded-xl">
                <Droplet predValue={forecast["value"]} className="w-[80px] h-[80px] absolute top-0 right-0 m-4"/>
                {/* Text Display */}
                <div className="flex flex-col w-full items-start self-start p-6">
                    <div className="flex flex-col items-center mb-6">
                        <h1 className="text-4xl self-start">Today</h1>
                        <p>{new Intl.DateTimeFormat("en-US", dayOptions).format(date)}</p>
                        <p>{date.toLocaleDateString()}</p>
                    </div>
                    <div className="flex flex-col items-start">
                        <p className="text-5xl">{forecast["value"]/10}%</p>
                        <p className="text-xs">models predict a bloom</p>
                    </div>
                </div>

                {/* Bar Chart */}
                <div className="flex flex-col w-full h-[40vh]">
                    <ResponsiveContainer width="50%" height={220} className="absolute bottom-0 right-0">
                        <BarChart
                            data={chartData}
                            margin={{ top: 15, right: 20, bottom: 10, left: 0 }}
                            barCategoryGap="20%"
                        >
                            <Bar dataKey="value" fill="var(--color-secondary)">
                                <LabelList
                                    dataKey="value"
                                    position="top"
                                    style={{ fill: "white", fontSize: "12px" }}
                                />
                            </Bar>
                            <XAxis dataKey="prediction" tick={{ fill: "white" }} interval={0} />
                        </BarChart>
                    </ResponsiveContainer>
                </div>
            </div>
        </div>
    );
}