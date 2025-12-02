import plotly.graph_objects as go
from plotly.subplots import make_subplots


def get_visualization(spark):
    activation_rate_df = spark.sql("""
    select cohort_month
    , total_users
    , activation_rate_pct
    from activation_rate 
    where year(cohort_month) = 2008
    order by cohort_month 
    """).toPandas()


    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Add bar chart for total_users (primary y-axis)
    fig.add_trace(
        go.Bar(x=activation_rate_df['cohort_month'], 
               y=activation_rate_df['total_users'],
               name='Total Users'),
        secondary_y=False
    )
    
    # Add line chart for activation_rate_pct (secondary y-axis)
    fig.add_trace(
        go.Scatter(x=activation_rate_df['cohort_month'], 
                   y=activation_rate_df['activation_rate_pct'],
                   name='Activation Rate %',
                   mode='lines+markers',
                   line=dict(color='red', width=2),
                   marker=dict(size=8)),
        secondary_y=True
    )
    
    # Update layout
    fig.update_layout(
        title='Total Users and Activation Rate by Cohort Month',
        xaxis_title='Cohort Month'
    )
    
    # Set y-axes titles
    fig.update_yaxes(title_text="Total Users", secondary_y=False)
    fig.update_yaxes(title_text="Activation Rate %", secondary_y=True)

    return fig
