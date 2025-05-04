'use client'

import React, { useState, FormEvent, useEffect } from 'react';
import { supabase } from '@/lib/supabaseClient';

// Define interfaces for input and result data
interface PredictionInput {
  category: string;
  app_size: string;
  app_type: 'Free' | 'Paid';
  price: number;
  content_rating: 'Everyone' | 'Teen' | 'Mature 17+' | 'Adults only 18+';
  genres: string;
}

interface PredictionResult extends PredictionInput {
  id?: number;
  predicted_installs: string;
  predicted_reviews: string;
  predicted_rating: string;
  created_at?: string;
}

export default function Home() {
  // State for form inputs
  const [inputData, setInputData] = useState<PredictionInput>({
    category: '',
    app_size: '',
    app_type: 'Free',
    price: 0,
    content_rating: 'Everyone',
    genres: '',
  });

  // State for the most recent prediction result
  const [latestPrediction, setLatestPrediction] = useState<PredictionResult | null>(null);
  // State for the history of predictions
  const [history, setHistory] = useState<PredictionResult[]>([]);
  // State to track loading status
  const [isLoading, setIsLoading] = useState<boolean>(false);
  // State for potential errors
  const [error, setError] = useState<string | null>(null);
  // State for history loading
  const [isHistoryLoading, setIsHistoryLoading] = useState<boolean>(true);

  const fetchHistory = async () => {
    setIsHistoryLoading(true);
    setError(null); // Clear previous errors
    try {
      // Add 1 second delay
      await new Promise(resolve => setTimeout(resolve, 10000));
      
      const { data, error: fetchError } = await supabase
        .from('prediction_history')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(50); // Limit results

      if (fetchError) {
        console.error("Supabase fetch error:", fetchError);
        throw new Error(fetchError.message || "Failed to fetch data from Supabase.");
      }

      // Map DB data (snake_case potentially) to frontend state
      const mappedHistory = data?.map(item => ({
          category: item.category,
          app_size: item.size, // DB `size` -> frontend `app_size`
          app_type: item.type, // DB `type` -> frontend `app_type`
          price: typeof item.price === 'string' ? parseFloat(item.price) : (item.price ?? 0), // Handle potential null price
          content_rating: item.content_rating,
          genres: item.genres,
          id: item.id,
          predicted_installs: item.predicted_installs,
          predicted_reviews: item.predicted_reviews,
          predicted_rating: item.predicted_rating,
          created_at: item.created_at,
      })) || [];
      if (mappedHistory.length > 0) {
        setLatestPrediction(mappedHistory[0]);
      }
      setHistory(mappedHistory as PredictionResult[]);

    } catch (err: any) {
      console.error("Error fetching prediction history:", err);
      setError(`Failed to load prediction history: ${err.message}`);
      setHistory([]);
    } finally {
      // Finish loading
      setIsHistoryLoading(false);
    }
  };

  useEffect(() => {
    // Set up real-time subscription
    const subscription = supabase
        .channel('prediction_changes')
        .on('postgres_changes', 
            {
                event: '*',
                schema: 'public',
                table: 'prediction_history'
            },
            (payload) => {
                // Fetch fresh data when changes occur
                fetchHistory();
            }
        )
        .subscribe();

    // Initial fetch
    fetchHistory();

    // Cleanup subscription
    return () => {
        subscription.unsubscribe();
    };
  }, []);

  // Handler for input changes
  const handleInputChange = (
    e: React.ChangeEvent<HTMLInputElement | HTMLSelectElement>
  ) => {
    const { name, value } = e.target;
    let processedValue: string | number = value;

    if (name === 'price') {
      processedValue = parseFloat(value) || 0;
    }
    if (name === 'app_type' && value === 'Free') {
        // Reset price to 0 when Type becomes Free
         setInputData(prevData => ({ ...prevData, price: 0, [name]: value }));
         return; // Exit early as state is set
    }

    setInputData((prevData) => ({
      ...prevData,
      [name]: processedValue,
    }));
  };

  // Handler for form submission
  const handleSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setIsLoading(true);
    setLatestPrediction(null);
    setError(null);

    console.log("Sending data to backend:", inputData);
   
    try {
      const response = await fetch('http://localhost:5001/predict', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(inputData),
      });

      if (!response.ok) {
          const errorData = await response.json().catch(() => ({ message: "Unknown error" }));
          throw new Error(errorData.error || `HTTP error! Status: ${response.status}`);
      }

      var predictionResult: PredictionResult = await response.json();
      console.log("Received prediction result from backend:", predictionResult);

     
      // Fetch fresh history data after prediction
      // Add 1 second delay before showing prediction and fetching history
      await new Promise(resolve => setTimeout(resolve, 1000));
      const { data: freshHistory, error: fetchError } = await supabase
        .from('prediction_history')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(50);

      if (fetchError) {
        throw new Error(fetchError.message || "Failed to fetch updated history.");
      }

      // Map fresh history data
      const mappedHistory = freshHistory?.map(item => ({
        category: item.category,
        app_size: item.size,
        app_type: item.type,
        price: typeof item.price === 'string' ? parseFloat(item.price) : (item.price ?? 0),
        content_rating: item.content_rating,
        genres: item.genres,
        id: item.id,
        predicted_installs: item.predicted_installs,
        predicted_reviews: item.predicted_reviews,
        predicted_rating: item.predicted_rating,
        created_at: item.created_at,
      })) || [];

      
      setHistory(mappedHistory as PredictionResult[]);
     
      if (mappedHistory.length > 0) {
        setLatestPrediction(mappedHistory[0]);
      }  // Set latest prediction after delay

    } catch (err: any) {
      console.error("Error during prediction processing:", err);
      setError(err.message || "An unexpected error occurred.");
      setLatestPrediction(null); // Clear latest prediction on error
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <main className="flex min-h-screen flex-col items-center p-6 md:p-16 bg-gray-800">
      <div className="w-full max-w-4xl">
        <h1 className="text-3xl md:text-4xl font-bold mb-8 text-center text-gray-100">Play Store Metrics Predictor</h1>

        {/* Form Section */}
        <form onSubmit={handleSubmit} className="w-full bg-white p-6 md:p-8 rounded-lg shadow-xl mb-10">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-4 mb-6">
            {/* Category Input */}
            <div>
              <label htmlFor="category" className="block text-sm font-medium text-gray-700 mb-1">
                Category <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                name="category"
                id="category"
                value={inputData.category}
                onChange={handleInputChange}
                required
                placeholder="e.g., GAME, PRODUCTIVITY"
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition duration-150 ease-in-out text-gray-900"
              />
            </div>

            {/* Genre Input - Changed name to 'genres' */}
            <div>
              <label htmlFor="genres" className="block text-sm font-medium text-gray-700 mb-1">
                Genres <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                name="genres"
                id="genres"
                value={inputData.genres}
                onChange={handleInputChange}
                required
                placeholder="e.g., Action;Puzzle;Finance"
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition duration-150 ease-in-out text-gray-900"
              />
            </div>

            {/* App Size Input */}
            <div>
              <label htmlFor="app_size" className="block text-sm font-medium text-gray-700 mb-1">
                App Size <span className="text-red-500">*</span>
              </label>
              <input
                type="text"
                name="app_size"
                id="app_size"
                value={inputData.app_size}
                onChange={handleInputChange}
                required
                placeholder="e.g., 15M, Varies with device"
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition duration-150 ease-in-out text-gray-900"
              />
            </div>

            {/* Content Rating Dropdown */}
            <div>
              <label htmlFor="content_rating" className="block text-sm font-medium text-gray-700 mb-1">
                Content Rating <span className="text-red-500">*</span>
              </label>
              <select
                name="content_rating"
                id="content_rating"
                value={inputData.content_rating}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 bg-white transition duration-150 ease-in-out appearance-none text-gray-900"
              >
                <option>Everyone</option>
                <option>Teen</option>
                <option>Mature 17+</option>
                <option>Adults only 18+</option>
              </select>
            </div>

            {/* App Type Dropdown */}
            <div>
              <label htmlFor="app_type" className="block text-sm font-medium text-gray-700 mb-1">
                App Type <span className="text-red-500">*</span>
              </label>
              <select
                name="app_type"
                id="app_type"
                value={inputData.app_type}
                onChange={handleInputChange}
                required
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 bg-white transition duration-150 ease-in-out appearance-none text-gray-900"
              >
                <option value="Free">Free</option>
                <option value="Paid">Paid</option>
              </select>
            </div>

            {/* Price Input (conditional styling/disabling) */}
            <div>
              <label htmlFor="price" className="block text-sm font-medium text-gray-700 mb-1">
                Price ($)
              </label>
              <input
                type="number"
                name="price"
                id="price"
                value={inputData.price}
                onChange={handleInputChange}
                min="0"
                step="0.01"
                required={inputData.app_type === 'Paid'} // Only required if Paid
                disabled={inputData.app_type === 'Free'}
                placeholder="0.00"
                className={`w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 transition duration-150 ease-in-out ${inputData.app_type === 'Free' ? 'bg-gray-100 cursor-not-allowed opacity-70' : 'bg-white'} text-gray-900`}
              />
            </div>
          </div>

          {/* Submit Button */}
          <button
            type="submit"
            disabled={isLoading}
            className="w-full inline-flex justify-center items-center py-2.5 px-4 border border-transparent shadow-sm text-base font-medium rounded-md text-white bg-indigo-600 hover:bg-indigo-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-indigo-500 disabled:opacity-60 disabled:cursor-not-allowed transition duration-150 ease-in-out"
          >
            {isLoading ? (
                <>
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Processing...
                </>
            ) : (
              'Get Prediction'
            )}
          </button>

           {/* Error Message Display */}
          {error && (
            <p className="mt-4 text-sm text-red-600 bg-red-100 p-3 rounded-md text-center">Error: {error}</p>
          )}
        </form>

        {/* Display Latest Prediction Result */}
        {latestPrediction && (
          <div className="w-full bg-white p-6 rounded-lg shadow-lg mb-10 border border-green-300 animate-fade-in">
            <h2 className="text-xl font-semibold mb-4 text-gray-700 border-b pb-2">Latest Prediction Result</h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4 text-sm text-gray-800">
              <div><strong className="font-medium text-gray-600">Category:</strong> {latestPrediction.category}</div>
              <div><strong className="font-medium text-gray-600">Genres:</strong> {latestPrediction.genres}</div>
              <div><strong className="font-medium text-gray-600">Size:</strong> {latestPrediction.app_size}</div>
              <div><strong className="font-medium text-gray-600">Type:</strong> {latestPrediction.app_type}</div>
              <div><strong className="font-medium text-gray-600">Price:</strong> {latestPrediction.price}</div>
              <div><strong className="font-medium text-gray-600">Content Rating:</strong> {latestPrediction.content_rating}</div>
            </div>
            <div className="mt-4 pt-4 border-t grid grid-cols-1 sm:grid-cols-3 gap-4 text-center">
                <div className="bg-indigo-50 p-3 rounded">
                    <p className="text-xs text-indigo-700 font-semibold uppercase tracking-wide">Predicted Rating</p>
                    <p className="text-lg font-bold text-indigo-900">{latestPrediction.predicted_rating}</p>
                </div>
                 <div className="bg-indigo-50 p-3 rounded">
                    <p className="text-xs text-indigo-700 font-semibold uppercase tracking-wide">Predicted Installs</p>
                    <p className="text-lg font-bold text-indigo-900">{latestPrediction.predicted_installs}</p>
                </div>
                 <div className="bg-indigo-50 p-3 rounded">
                    <p className="text-xs text-indigo-700 font-semibold uppercase tracking-wide">Predicted Reviews</p>
                    <p className="text-lg font-bold text-indigo-900">{latestPrediction.predicted_reviews}</p>
                </div>
            </div>
          </div>
        )}

        {/* --- UNCOMMENT History Display --- */}
        <div className="w-full bg-white p-6 md:p-8 rounded-lg shadow-lg">
          <h2 className="text-2xl font-semibold mb-5 text-gray-700 border-b pb-3">Prediction History</h2>
          {isHistoryLoading ? ( 
             <p className="text-gray-500 italic text-center py-4">Loading history...</p>
          ) : error && history.length === 0 ? ( // Display error only if history failed AND is empty
             <p className="text-red-500 text-center py-4">Error: {error}</p>
          ) : history.length === 0 ? (
            <p className="text-gray-500 italic text-center py-4">No predictions made yet.</p>
          ) : (
            <div className="space-y-4 max-h-96 overflow-y-auto pr-2">
              {/* Ensure item.id is used as key */} 
              {history.map((item) => (
                 <div key={item.id ?? Math.random()} className="bg-gray-50 p-4 rounded-lg shadow-sm border border-gray-200 hover:shadow-md transition-shadow duration-200">
                   <p className="text-xs text-gray-500 mb-2">{item.created_at ? new Date(item.created_at).toLocaleString() : 'Date unavailable'}</p>
                   <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-x-4 gap-y-1 text-sm text-gray-800">
                       <div><strong className="font-medium text-gray-600">Category:</strong> {item.category}</div>
                       <div><strong className="font-medium text-gray-600">Genres:</strong> {item.genres}</div>
                       {/* Display app_size, app_type from state */} 
                       <div><strong className="font-medium text-gray-600">Size:</strong> {item.app_size}</div> 
                       <div><strong className="font-medium text-gray-600">Type:</strong> {item.app_type}</div>
                       <div className="font-semibold text-indigo-700"><strong className="font-medium text-indigo-600">Rating:</strong> {item.predicted_rating}</div>
                       <div className="font-semibold text-indigo-700"><strong className="font-medium text-indigo-600">Installs:</strong> {item.predicted_installs}</div>
                       <div className="font-semibold text-indigo-700"><strong className="font-medium text-indigo-600">Reviews:</strong> {item.predicted_reviews}</div>
                   </div>
                 </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </main>
  );
}

// Basic fade-in animation (add to globals.css or here in style jsx)
// You might need to configure Tailwind for animations
// Example for globals.css:
/*
@tailwind base;
@tailwind components;
@tailwind utilities;

@layer utilities {
  @keyframes fadeIn {
    from { opacity: 0; transform: translateY(10px); }
    to { opacity: 1; transform: translateY(0); }
  }
  .animate-fade-in {
    animation: fadeIn 0.5s ease-out forwards;
  }
}
*/
