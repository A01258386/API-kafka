import React, { useEffect, useState } from 'react'
import '../App.css';

export default function AppStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null)

	const getStats = () => {
	
        fetch(`http://kafka.westus3.cloudapp.azure.com:8100/stats`)
            .then(res => res.json())
            .then((result)=>{
				console.log("Received Stats")
                setStats(result);
                setIsLoaded(true);
            },(error) =>{
                setError(error)
                setIsLoaded(true);
            })
    }
    useEffect(() => {
		const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
		return() => clearInterval(interval);
    }, [getStats]);

    if (error){
        return (<div className={"error"}>Error found when fetching from API</div>)
    } else if (isLoaded === false){
        return(<div>Loading...</div>)
    } else if (isLoaded === true){
        return(
            <div>
                <h1>Latest Stats</h1>
                <table className={"StatsTable"}>
					<tbody>
						<tr>
							<th>Fly and Drive Stats</th>
						</tr>
						<tr>
							<td colspan="2">Average Speed: {stats['average_speed']}</td>
						</tr>
						<tr>
							<td colspan="2">Average Altitue: {stats['average_altitue']}</td>
						</tr>
						<tr>
							<td colspan="2">Average Air Pressure: {stats['average_air_pressure']}</td>
						</tr>
                        <tr>
                            <td colspan="2">Average Weight: {stats['average_weight']}</td>
                        </tr>
					</tbody>
                </table>
                <h3>Last Updated: {stats['timestamp']}</h3>

            </div>
        )
    }
}
