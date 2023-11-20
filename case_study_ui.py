import ipywidgets as widgets
from IPython.display import display, clear_output
from case_study_utils import CorrCommunity
import qgrid

def show_df(df):
    display_attrs = [
        "tbl_name1",
        "align_attrs1",
        "agg_attr1",
        "tbl_id2",
        "tbl_name2",
        "align_attrs2",
        "agg_attr2",
        "missing_ratio_o2",
        "r_val",
        "samples",
    ]
    qgrid_widget = qgrid.show_grid(
        df[display_attrs],
        precision=2,
        grid_options={"forceFitColumns": False, "defaultColumnWidth": 150},
    )
    display(qgrid_widget)

def show_communities(corr_community: CorrCommunity, show_corr_in_same_tbl):
    clusters = corr_community.all_communities
    # Function to be triggered when the dropdown value changes
    def on_dropdown_change(change):
        if change["type"] == "change" and change["name"] == "value":
            with main_output:
                clear_output(wait=True)
                cluster_name = change["new"]
                print(cluster_name)

                for table in clusters[cluster_name]:
                    print(f"  - {table}")

                # Create a button and set its click action for the selected cluster
                btn = widgets.Button(description="Show Variables")
                btn.on_click(
                    lambda change, cluster_name=cluster_name: on_button_click(
                        cluster_name, change
                    )
                )

                btn_corr = widgets.Button(description="Show Correlations")
                btn_corr.on_click(
                    lambda change, cluster_name=cluster_name: on_corr_button_click(
                        cluster_name, change
                    )
                )

                display(btn)
                display(btn_corr)
            with variable_output:
                clear_output()
            with corr_output:
                clear_output()

    # Function to be triggered when the button is clicked
    def on_button_click(cluster_name, btn_object):
        with variable_output:
            clear_output(wait=True)
            cluster = clusters[cluster_name]
            for table, variables in cluster.items():
                print(f"{table}")
                for var in variables:
                    print(f" - {var}")
                # print(f"{table}: {', '.join(variables)}")

    def on_corr_button_click(cluster_name, btn_object):
        with corr_output:
            clear_output(wait=True)
            cluster_id = int(cluster_name.split()[1])
            res = corr_community.get_corr_in_cluster_i(
                cluster_id, show_corr_in_same_tbl
            )
            display(f"{cluster_name} has {len(res)} correlations")

            qgrid_widget = qgrid.show_grid(
                res,
                precision=2,
                grid_options={"forceFitColumns": False, "defaultColumnWidth": 150},
            )

            def handle_selection_change(change):
                selected_rows = qgrid_widget.get_changed_df()
                with cnt_output:
                    clear_output(wait=True)
                    display(f"#filtered rows: {len(selected_rows)}")

            # qgrid_widget.on("filter_changed", handle_selection_change)
            qgrid_widget.observe(handle_selection_change, names=["_selected_rows"])
            cnt_output = widgets.Output()
            with cnt_output:
                clear_output(wait=True)
                display(f"#filtered rows: {len(qgrid_widget.get_changed_df())}")
            print("should display")
            display(qgrid_widget)
            display(cnt_output)

    # Create the dropdown widget and set the initial value
    dropdown = widgets.Dropdown(
        options=list(clusters.keys()),
        description="Show:",
        layout=widgets.Layout(
            width="200px"
        ),  # Adjust width to fit the dropdown content
    )
    dropdown.observe(on_dropdown_change)

    # Create output widgets to hold main content and variable details
    main_output = widgets.Output()
    variable_output = widgets.Output()
    corr_output = widgets.Output()

    # Display dropdown and outputs
    display(dropdown)
    display(main_output)
    display(variable_output)
    display(corr_output)

    # Trigger the display for the default cluster (Cluster 1) when the cell is run
    on_dropdown_change({"type": "change", "name": "value", "new": "Community 0"})
