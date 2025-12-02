import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

def get_visualization(spark):
    df = spark.sql("""
    select tag
    , avg(AnswerCount) as avg_answercount
        , avg(score) as avg_score
        , round(avg(engagement_velocity_score),2) as avg_engagement_velocity_score
    from engagement_velocity
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

    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df['avg_score'],
        y=df['avg_answercount'],
        mode='markers',  # Remove 'text'
        marker=dict(
            size=df['avg_engagement_velocity_score'] / 1000,
            color=df['avg_engagement_velocity_score'],
            colorscale='Viridis',
            showscale=True,
            colorbar=dict(title="Engagement<br>Velocity"),
            line=dict(width=1, color='white')  # Add border for clarity
        ),
        text=df['tag'],  # Still available on hover
        hovertemplate='<b>%{text}</b><br>Score: %{x}<br>Answer Count: %{y}<extra></extra>',
        name='Tags'
    ))
    
    fig.update_layout(
        title='Tag Performance: Answer Count vs Score',
        xaxis_title='Average Score',
        yaxis_title='Average Answer Count',
        hovermode='closest'
    )
    
    return fig
