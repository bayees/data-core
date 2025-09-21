from typing import Generator, Any, Dict, List
import dlt
from calendra.europe import Denmark
from datetime import datetime, timedelta

@dlt.resource(
    table_format="delta",
    write_disposition="merge",
    primary_key="date_actual",
)
def calendar() -> Generator[List[Dict[str, Any]], Any, Any]:
    current_year = datetime.today().year
    years = list(range(current_year - 10, current_year + 2, 1))
    
    # Generate date range
    start_date = datetime(years[0], 1, 1)
    end_date = datetime(years[-1], 12, 31)
    calendar_dates = []
    current_date = start_date
    
    while current_date <= end_date:
        date_info = {
            'date_actual': current_date,
            'day_actual': str(current_date.day),
            'day_zero_added': f"{current_date.day:02d}",
            'weekday_actual': current_date.isoweekday(),
            'weekday_name_short': current_date.strftime('%a'),
            'weekday_name_long': current_date.strftime('%A'),
            'month_actual': str(current_date.month),
            'month_zero_added': f"{current_date.month:02d}",
            'month_name_short': current_date.strftime('%b'),
            'month_name_long': current_date.strftime('%B'),
            'quarter_actual': (current_date.month - 1) // 3 + 1,
            'quarter_name': f"Q{(current_date.month - 1) // 3 + 1}",
            'year_quarter_name': f"{current_date.year}-Q{(current_date.month - 1) // 3 + 1}",
            'day_of_year': int(current_date.strftime('%j')),
            'iso_week_of_year': current_date.strftime('%V'),
            'iso_year': current_date.strftime('%G'),
            'year': str(current_date.year),
            'first_day_of_month': current_date.replace(day=1),
            'end_of_month': (current_date.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1),
            'first_day_of_year': current_date.replace(month=1, day=1),
        }
        
        # Add holidays
        cal = Denmark()
        date_info['is_working_day'] = cal.is_working_day(current_date)
        
        # Check if date is a holiday
        holidays = cal.holidays(current_date.year)
        date_info['holiday'] = next((name for date, name in holidays if date == current_date.date()), None)
        
        calendar_dates.append(date_info)
        current_date += timedelta(days=1)

    yield calendar_dates

@dlt.source
def calendar_source():  # type: ignore
    return [calendar]

def run_source() -> None:
    # configure the pipeline with your destination details
    pipeline = dlt.pipeline(
        pipeline_name="calendar_pipeline",
        destination='filesystem',
        dataset_name="calendar"
    )

    # run the pipeline with your parameters
    load_info = pipeline.run(calendar_source())

    # pretty print the information on data that was loaded
    print(load_info)  # noqa: T201

if __name__ == "__main__":
    run_source()