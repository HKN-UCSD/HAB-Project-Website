import Image from "next/image";
import Navbar from "../components/Navbar";
import TopBar from "../components/TopBar";
import Today from "../components/Today";
import Graph from "../components/Graph";

export default async function Home() {
  // TODO: Fetch forecasts from backend
  //let forecasts = await getForecasts();
  let forecasts = [
    { "day": "Sunday", "value": 760 },
    { "day": "Monday", "value": 260 },
    { "day": "Tuesday", "value": 10 },
    { "day": "Wednesday", "value": 60 },
    { "day": "Thursday", "value": 400 },
    { "day": "Friday", "value": 560 },
    { "day": "Saturday", "value": 920 }
  ]


  return (
    <main className="min-h-screen h-auto bg-[url(/background.png)] bg-cover bg-no-repeat bg-fixed items-center">
      <Image
          src="/hknlogo.svg"
          className="pl-10 pt-8"
          alt="HKN Logo"
          width={180}
          height={50}
      />
      <h1 className="text-4xl font-bold text-center text-secondary shadow">
        Projected Bioluminescence Forecasts
      </h1>
      <div className="flex flex-row mt-[3vh] justify-center">
        <Image
          src="/location.svg"
          alt="Location Pin"
          width={25}
          height={25}
          className="mr-4"
        />
        <h2 className="text-xl text-white text-center">
          Scripps Pier, La Jolla
        </h2>
      </div>
      <h2 className="text-center text-white mt-[5vh] mb-4">
        7-Day Predictions: {
          (() => {
            const today = new Date();
            const sunday = new Date(today);
            const saturday = new Date(today);
            sunday.setDate(today.getDate() - today.getDay());
            saturday.setDate(today.getDate() + (6 - today.getDay()));
      
            const format = (date) =>
              date.toLocaleDateString("en-US", { month: "long", day: "numeric" });
      
            return `Sun, ${format(sunday)} – Sat, ${format(saturday)}`;
          })()
        }
      </h2>
      <div className="flex items-center justify-center mb-4">
        <TopBar forecasts={forecasts} className=""/>
      </div>
      <div className="flex flex-col md:flex-row justify-center items-center space-x-0 md:space-x-4 gap-y-4 md:gap-y-0">
        <Today forecasts={forecasts} className="md:self-start"/>
        <Graph forecasts={forecasts} className="mb-[5vh]"/>
      </div>


      <div className = "flex flex-col justify-center items-center">
        <h1 className="text-2xl text-center mt-[5vh]  text-secondary font-bold " >About</h1>
        <p className="text-center text-white opacity-80 mt-[5vh] mb-[5vh] max-w-2xl leading-loose">
          This project is a web application that provides a 7-day forecast of bioluminescence levels at Scripps Pier, La Jolla. The forecasts are based on data collected from various sources and are displayed in an easy-to-read format.
          The application features a top bar that shows the forecast for each day, a detailed view of today's forecast, and a graph that visualizes the data over the week. The goal of this project is to help users plan their visits to Scripps Pier based on the bioluminescence levels.
          
          The application is built using Next.js and Tailwind CSS, providing a responsive and user-friendly interface. The data is fetched from a backend server and displayed in real-time, ensuring that users have the most up-to-date information.
          The project is still in development, and future updates will include more features and improvements to the user experience. We welcome feedback and suggestions from users to help us make this application even better.  
          </p>
      </div>
    </main>
  );
}
