import matplotlib.pyplot as plt
import pandas as pd

def plot_avg_temp_by_year(csv_file):
    df = pd.read_csv(csv_file)
    avg_temp_by_year = df.groupby('year')['max_temp'].mean()
    avg_f_temp_by_year = df.groupby('year')['f_max_temp'].mean()

    plt.figure(figsize=(10, 6))

    plt.subplot(2, 1, 1)
    plt.plot(avg_temp_by_year.index, avg_temp_by_year.values, label='Average Temperature (°C)', marker='o', color='blue')
    for x, y in zip(avg_temp_by_year.index, avg_temp_by_year.values):
        plt.text(x, y, f'{y:.2f} °C', ha='center', va='bottom')
    plt.title('Average Temperature (°C) by Year')
    plt.xlabel('Year')
    plt.ylabel('Average Temperature (°C)')
    plt.legend()
    plt.grid(True)

    plt.subplot(2, 1, 2)
    plt.plot(avg_f_temp_by_year.index, avg_f_temp_by_year.values, label='Average Temperature (°F)', marker='o', color='red')
    for x, y in zip(avg_f_temp_by_year.index, avg_f_temp_by_year.values):
        plt.text(x, y, f'{y:.2f} °F', ha='center', va='bottom')
    plt.title('Average Temperature (°F) by Year')
    plt.xlabel('Year')
    plt.ylabel('Average Temperature (°F)')
    plt.legend()
    plt.grid(True)

    plt.tight_layout()
    plt.show()

# Visualises the avg.temp change year-by-year
plot_avg_temp_by_year("path_to_weather_data_output.csv")
