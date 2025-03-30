# ADD Project Description
This project uses a dataset from the Google Play Store, which includes various features such as category, app size, app type, price, content rating, and genre. The goal is to predict how many installs and reviews an app will have, as well as the app's rating. The project mimics a test environment for understanding and experimenting with how the Google Play Store algorithm might work, utilizing data science and machine learning techniques to make predictions.

### Architecture:

- **Producer**: A Python script that supplies the data from the Google Play Store dataset.
- **Processor**: A Python script to clean, transform, and normalize the data.
- **Uploader**: A Python script that uploads the processed data to the database (MySQL/PostgreSQL).
- **Database**: Stores both clean and raw datasets, along with predictions.
- **Presenter**: A simple user interface (UI) or web frontend where users can input app details for prediction and view results.

Communication:
All components communicate through RabbitMQ for seamless data flow and interaction.
