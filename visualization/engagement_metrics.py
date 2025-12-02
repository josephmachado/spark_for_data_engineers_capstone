import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

def get_visualization(spark):
    df = spark.sql("""
    select tag
    , avg(AnswerCount) as avg_answercount
        , avg(score) as avg_score
        , round(avg(engagement_velocity_score),2) as avg_engagement_velocity_score
    from stackoverflow.engagement_velocity
    LATERAL VIEW EXPLODE(
                SPLIT(Tags, '\\\\|')
            ) t1 AS tag
    group by tag
        order by 2 desc
        limit 20
        """).toPandas()
    
    # Convert to numeric types
    df['avg_answercount'] = pd.to_numeric(df['avg_answercount'])
    df['avg_score'] = pd.to_numeric(df['avg_score'])
    df['avg_engagement_velocity_score'] = pd.to_numeric(df['avg_engagement_velocity_score'])
    
    # Debug: Print the values to see what you're working with
    print("Engagement velocity score range:", 
          df['avg_engagement_velocity_score'].min(), 
          "to", 
          df['avg_engagement_velocity_score'].max())

    # Scale the marker sizes appropriately
    # Option 1: Use a multiplier instead of dividing
    marker_sizes = df['avg_engagement_velocity_score'] * 2  # Adjust multiplier as needed
    
    # Option 2: Normalize to a reasonable range (e.g., 10-100)
    min_size = 10
    max_size = 100
    if df['avg_engagement_velocity_score'].max() > df['avg_engagement_velocity_score'].min():
        normalized_sizes = min_size + (max_size - min_size) * (
            (df['avg_engagement_velocity_score'] - df['avg_engagement_velocity_score'].min()) / 
            (df['avg_engagement_velocity_score'].max() - df['avg_engagement_velocity_score'].min())
        )
    else:
        normalized_sizes = [50] * len(df)  # Default size if all values are the same
    
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['avg_score'],
        y=df['avg_answercount'],
        mode='markers',
        marker=dict(
            size=normalized_sizes,  # Use normalized sizes
            color=df['avg_engagement_velocity_score'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Engagement<br>Velocity"),
            line=dict(width=1, color='white'),
            sizemode='diameter'  # or 'area' depending on preference
        ),
        text=df['tag'],
        hovertemplate='<b>%{text}</b><br>' +
                      'Score: %{x:.2f}<br>' +
                      'Answer Count: %{y:.2f}<br>' +
                      'Engagement Velocity: %{marker.color:.2f}' +
                      '<extra></extra>',
        name='Tags'
    ))
    
    fig.update_layout(
        title='Tag Performance: Answer Count vs Score',
        xaxis_title='Average Score',
        yaxis_title='Average Answer Count',
        hovermode='closest',
        width=900,
        height=600
    )
    
    return fig
