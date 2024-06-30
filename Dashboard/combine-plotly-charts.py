import plotly.graph_objects as go
from plotly.subplots import make_subplots
import math


def combine_and_save_charts(
    figures,
    output_path,
    num_columns=2,
    title="Combined Dashboard",
    legend_orientation="h",
    color_scheme=None,
):
    """
    Combines multiple Plotly figures into a single dashboard and saves it as an HTML file.

    Args:
    figures (list): List of plotly.graph_objects.Figure objects
    output_path (str): Path to save the output HTML file
    num_columns (int): Number of columns in the grid layout (default: 2)
    title (str): Title of the combined dashboard (default: "Combined Dashboard")
    legend_orientation (str): Orientation of the legend, 'h' for horizontal or 'v' for vertical (default: 'h')
    color_scheme (list): List of colors for a custom color scheme (default: None)

    Returns:
    None
    """
    if not isinstance(figures, list) or len(figures) == 0:
        raise ValueError(
            "Input 'figures' must be a non-empty list of Plotly Figure objects"
        )

    num_figures = len(figures)
    num_rows = math.ceil(num_figures / num_columns)

    # Calculate layout based on content
    max_legend_items = max(len(fig.data) for fig in figures)
    legend_space = min(100 + 20 * max_legend_items, 200)  # Cap at 200px

    # Create subplot grid
    fig = make_subplots(
        rows=num_rows,
        cols=num_columns,
        subplot_titles=[fig.layout.title.text for fig in figures],
        vertical_spacing=0.1,
        horizontal_spacing=0.05,
    )

    # Add each figure to the subplot
    for index, figure in enumerate(figures):
        row = index // num_columns + 1
        col = index % num_columns + 1

        for trace in figure.data:
            fig.add_trace(trace, row=row, col=col)

        # Update axes properties for this subplot
        fig.update_xaxes(title_text=figure.layout.xaxis.title.text, row=row, col=col)
        fig.update_yaxes(title_text=figure.layout.yaxis.title.text, row=row, col=col)

    # Set up legend parameters based on orientation
    if legend_orientation == "h":
        legend_params = dict(
            orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5
        )
        height_adjustment = legend_space
        width_adjustment = 0
    else:  # vertical orientation
        legend_params = dict(
            orientation="v", yanchor="middle", y=0.5, xanchor="right", x=1.05
        )
        height_adjustment = 0
        width_adjustment = legend_space

    # Update layout
    fig.update_layout(
        title_text=title,
        height=350 * num_rows + height_adjustment,
        width=600 * num_columns + width_adjustment,
        showlegend=True,
        legend=legend_params,
    )

    # Apply custom color scheme if provided
    if color_scheme:
        for trace in fig.data:
            trace.update(
                marker=dict(
                    color=color_scheme[fig.data.index(trace) % len(color_scheme)]
                )
            )

    # Adjust subplot positions
    fig.update_layout(margin=dict(t=50, b=legend_space, r=50))

    # Save as HTML
    try:
        fig.write_html(output_path)
        print(f"Dashboard saved to {output_path}")
    except Exception as e:
        print(f"Error saving dashboard: {str(e)}")


# Example usage:
# figures_list = [fig1, fig2, fig3, fig4]
# custom_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
# combine_and_save_charts(figures_list, "combined_dashboard.html", num_columns=2,
#                         title="Process Mining Dashboard", legend_orientation='v',
#                         color_scheme=custom_colors)
