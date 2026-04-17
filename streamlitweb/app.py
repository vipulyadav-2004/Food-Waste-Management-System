import streamlit as st
import pandas as pd
import sys
import importlib
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from SQL import database as db
db = importlib.reload(db)

db.init_db()

st.set_page_config(page_title="Food Wastage Management", layout="wide")

st.sidebar.title("🍱 FoodBridge Portal")
menu = [
    "Home",
    "Donor Portal",
    "Manage Listings",
    "Receiver Portal",
    "Claims History",
    "Manage Claims",
    "SQL Insights",
]
choice = st.sidebar.selectbox("Menu", menu)

if choice == "Home":
    st.title("📊 Impact Dashboard")
    
    # Quick metrics
    df = db.get_available_food()
    total_items = len(df)
    total_qty = df['Quantity'].sum() if len(df) > 0 else 0
    
    # Get claims data
    claims_df = db.get_all_claims()
    total_claims = len(claims_df)
    pending_claims = len(claims_df[claims_df['Status'] == 'Pending']) if len(claims_df) > 0 else 0
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Available Listings", total_items)
    col2.metric("Total Units Available", int(total_qty))
    col3.metric("Total Claims", total_claims)
    col4.metric("Pending Claims", pending_claims)
    
    st.divider()
    
    # Row 1: Availability by City and Food Type
    col1, col2 = st.columns(2)
    
    with col1:
        if len(df) > 0:
            city_data = df[df['Quantity'] > 0].groupby('City')['Quantity'].sum().reset_index().sort_values('Quantity', ascending=False)
            if len(city_data) > 0:
                fig_city = px.bar(
                    city_data,
                    x='City',
                    y='Quantity',
                    title='Food Availability by City',
                    labels={'City': 'City', 'Quantity': 'Total Units'},
                    color='Quantity',
                    color_continuous_scale='Viridis'
                )
                fig_city.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_city, use_container_width=True)
            else:
                st.info("No city data available")
        else:
            st.info("No food listings available")
    
    with col2:
        if len(df) > 0:
            food_type_data = df[df['Quantity'] > 0].groupby('Food_Type')['Quantity'].sum().reset_index().sort_values('Quantity', ascending=False)
            if len(food_type_data) > 0:
                fig_food_type = px.pie(
                    food_type_data,
                    names='Food_Type',
                    values='Quantity',
                    title='Food Distribution by Type',
                    hole=0.3
                )
                fig_food_type.update_layout(height=400)
                st.plotly_chart(fig_food_type, use_container_width=True)
            else:
                st.info("No food type data available")
        else:
            st.info("No food listings available")
    
    # Row 2: Meal Type and Provider Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        if len(df) > 0:
            meal_type_data = df[df['Quantity'] > 0].groupby('Meal_Type')['Quantity'].sum().reset_index().sort_values('Quantity', ascending=False)
            if len(meal_type_data) > 0:
                fig_meal_type = px.bar(
                    meal_type_data,
                    x='Meal_Type',
                    y='Quantity',
                    title='Food Availability by Meal Type',
                    labels={'Meal_Type': 'Meal Type', 'Quantity': 'Total Units'},
                    color='Quantity',
                    color_continuous_scale='Blues'
                )
                fig_meal_type.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_meal_type, use_container_width=True)
            else:
                st.info("No meal type data available")
        else:
            st.info("No food listings available")
    
    with col2:
        if len(df) > 0:
            provider_data = df[df['Quantity'] > 0].groupby('Provider_Name')['Quantity'].sum().reset_index().sort_values('Quantity', ascending=False).head(10)
            if len(provider_data) > 0:
                fig_provider = px.bar(
                    provider_data,
                    y='Provider_Name',
                    x='Quantity',
                    title='Top 10 Providers by Available Units',
                    labels={'Provider_Name': 'Provider', 'Quantity': 'Total Units'},
                    color='Quantity',
                    color_continuous_scale='Greens',
                    orientation='h'
                )
                fig_provider.update_layout(height=400, showlegend=False)
                st.plotly_chart(fig_provider, use_container_width=True)
            else:
                st.info("No provider data available")
        else:
            st.info("No food listings available")
    
    # Row 3: Claims Status and Recent Activity
    col1, col2 = st.columns(2)
    
    with col1:
        if len(claims_df) > 0:
            status_data = claims_df['Status'].value_counts().reset_index()
            status_data.columns = ['Status', 'Count']
            fig_status = px.pie(
                status_data,
                names='Status',
                values='Count',
                title='Claims by Status',
                color_discrete_map={
                    'Pending': '#FFA500',
                    'Completed': '#28a745',
                    'Rejected': '#dc3545',
                    'Cancelled': '#6c757d'
                }
            )
            fig_status.update_layout(height=400)
            st.plotly_chart(fig_status, use_container_width=True)
        else:
            st.info("No claims data available")
    
    with col2:
        if len(df) > 0:
            expiry_data = df[df['Quantity'] > 0].copy()
            expiry_data['Days_Until_Expiry'] = pd.to_datetime(expiry_data['Expiry_Date']).apply(
                lambda x: (x - pd.Timestamp.now()).days
            )
            expiry_bins = pd.cut(expiry_data['Days_Until_Expiry'], bins=[-float('inf'), 0, 3, 7, 30, float('inf')], labels=['Expired', '0-3 Days', '3-7 Days', '7-30 Days', '30+ Days'])
            expiry_count = expiry_bins.value_counts().reset_index()
            expiry_count.columns = ['Days_Category', 'Count']
            expiry_count = expiry_count.sort_values('Days_Category')
            
            fig_expiry = px.bar(
                expiry_count,
                x='Days_Category',
                y='Count',
                title='Food Items by Expiry Timeline',
                labels={'Days_Category': 'Expiry Category', 'Count': 'Number of Items'},
                color='Count',
                color_continuous_scale='Reds'
            )
            fig_expiry.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig_expiry, use_container_width=True)
        else:
            st.info("No food listings available")
    
    # Row 4: Provider Type Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        if len(df) > 0:
            provider_type_data = df[df['Quantity'] > 0].groupby('Provider_Type')['Quantity'].sum().reset_index().sort_values('Quantity', ascending=False)
            if len(provider_type_data) > 0:
                fig_provider_type = px.pie(
                    provider_type_data,
                    names='Provider_Type',
                    values='Quantity',
                    title='Food Distribution by Provider Type'
                )
                fig_provider_type.update_layout(height=400)
                st.plotly_chart(fig_provider_type, use_container_width=True)
            else:
                st.info("No provider type data available")
        else:
            st.info("No food listings available")
    
    with col2:
        if len(claims_df) > 0:
            claims_by_receiver_type = claims_df['Receiver_Type'].value_counts().reset_index()
            claims_by_receiver_type.columns = ['Receiver_Type', 'Claims']
            fig_receiver_type = px.bar(
                claims_by_receiver_type,
                x='Receiver_Type',
                y='Claims',
                title='Claims Distribution by Receiver Type',
                labels={'Receiver_Type': 'Receiver Type', 'Claims': 'Number of Claims'},
                color='Claims',
                color_continuous_scale='Purples'
            )
            fig_receiver_type.update_layout(height=400, showlegend=False)
            st.plotly_chart(fig_receiver_type, use_container_width=True)
        else:
            st.info("No claims data available")

elif choice == "Donor Portal":
    st.title("🍎 List Surplus Food")
    with st.form("donor_form"):
        f_name = st.text_input("Food Item Name")
        qty = st.number_input("Quantity", min_value=1)
        expiry_date = st.date_input("Expiry Date")
        p_name = st.text_input("Your Restaurant/Organization Name")
        provider_type = st.selectbox(
            "Provider Type",
            ["Select Provider Type", "Restaurant", "NGO", "Individual", "Supermarket", "Catering Service"],
        )
        address = st.text_input("Address")
        city = st.text_input("City")
        contact = st.text_input("Contact")
        food_type = st.selectbox(
            "Food Type",
            ["Select Food Type", "Veg", "Non-Veg", "Vegan", "Mixed"],
        )
        meal_type = st.selectbox(
            "Meal Type",
            ["Select Meal Type", "Breakfast", "Lunch", "Dinner", "Snacks"],
        )

        if st.form_submit_button("Submit Listing"):
            if (
                not f_name.strip()
                or not p_name.strip()
                or not address.strip()
                or not city.strip()
                or not contact.strip()
                or provider_type == "Select Provider Type"
                or food_type == "Select Food Type"
                or meal_type == "Select Meal Type"
            ):
                st.error("Please fill all fields and select dropdown values.")
            else:
                db.add_food_listing(
                    f_name,
                    qty,
                    expiry_date.strftime("%Y-%m-%d"),
                    p_name,
                    provider_type,
                    address,
                    city,
                    contact,
                    food_type,
                    meal_type,
                )
                st.success(f"Listed {f_name} successfully!")

elif choice == "Manage Listings":
    st.title("🛠 Manage Food Listings")
    listings_df = db.get_all_food_listings()

    if listings_df.empty:
        st.info("No listings found yet. Add one from Donor Portal.")
    else:
        st.dataframe(listings_df, use_container_width=True)

    st.subheader("Update Listing")
    with st.form("update_form"):
        u_food_id = st.number_input("Food ID to Update", min_value=1, step=1)
        u_f_name = st.text_input("Food Item Name", key="u_f_name")
        u_qty = st.number_input("Quantity", min_value=1, key="u_qty")
        u_expiry_date = st.date_input("Expiry Date", key="u_expiry_date")
        u_p_name = st.text_input("Provider Name", key="u_p_name")
        u_provider_type = st.selectbox(
            "Provider Type",
            ["Select Provider Type", "Restaurant", "NGO", "Individual", "Supermarket", "Catering Service"],
            key="u_provider_type",
        )
        u_address = st.text_input("Address", key="u_address")
        u_city = st.text_input("City", key="u_city")
        u_contact = st.text_input("Contact", key="u_contact")
        u_food_type = st.selectbox(
            "Food Type",
            ["Select Food Type", "Veg", "Non-Veg", "Vegan", "Mixed"],
            key="u_food_type",
        )
        u_meal_type = st.selectbox(
            "Meal Type",
            ["Select Meal Type", "Breakfast", "Lunch", "Dinner", "Snacks"],
            key="u_meal_type",
        )

        if st.form_submit_button("Update Listing"):
            if (
                not u_f_name.strip()
                or not u_p_name.strip()
                or not u_address.strip()
                or not u_city.strip()
                or not u_contact.strip()
                or u_provider_type == "Select Provider Type"
                or u_food_type == "Select Food Type"
                or u_meal_type == "Select Meal Type"
            ):
                st.error("Please fill all fields and select dropdown values.")
            else:
                updated = db.update_food_listing(
                    u_food_id,
                    u_f_name,
                    u_qty,
                    u_expiry_date.strftime("%Y-%m-%d"),
                    u_p_name,
                    u_provider_type,
                    u_address,
                    u_city,
                    u_contact,
                    u_food_type,
                    u_meal_type,
                )
                if updated:
                    st.success(f"Listing {u_food_id} updated successfully.")
                else:
                    st.error("Food ID not found. Please enter a valid Food ID.")

    st.subheader("Delete Listing")
    with st.form("delete_form"):
        d_food_id = st.number_input("Food ID to Delete", min_value=1, step=1, key="d_food_id")
        if st.form_submit_button("Delete Listing"):
            deleted = db.delete_listing(d_food_id)
            if deleted:
                st.success(f"Listing {d_food_id} deleted successfully.")
            else:
                st.error("Food ID not found. Please enter a valid Food ID.")

elif choice == "Receiver Portal":
    st.title("🤝 Claim Food")
    filter_options = db.get_filter_options()

    st.subheader("Filter Available Food")
    col1, col2, col3, col4 = st.columns(4)
    selected_city = col1.selectbox("City", ["All"] + filter_options['cities'])
    selected_provider = col2.selectbox("Provider", ["All"] + filter_options['providers'])
    selected_food_type = col3.selectbox("Food Type", ["All"] + filter_options['food_types'])
    selected_meal_type = col4.selectbox("Meal Type", ["All"] + filter_options['meal_types'])

    active_filters = {
        'city': selected_city,
        'provider': selected_provider,
        'food_type': selected_food_type,
        'meal_type': selected_meal_type,
    }

    df = db.get_filtered_available_food(active_filters)
    st.dataframe(df, use_container_width=True)

    st.subheader("Provider Contacts For Direct Coordination")
    contacts_df = db.get_provider_contacts(active_filters)
    if contacts_df.empty:
        st.info("No provider contacts found for selected filters.")
    else:
        st.dataframe(contacts_df, use_container_width=True)
    
    # Simple Claim Logic
    food_id = st.number_input("Enter Food ID to Claim", min_value=1, step=1)
    claim_qty = st.number_input("How many units?", min_value=1, step=1)
    
    if st.button("Confirm Claim"):
        # You would collect receiver name/contact from inputs here
        success = db.claim_food(food_id, "Helping Hands NGO", "NGO", "9998887776", "Indore", claim_qty)
        if success:
            st.success("Claim processed! Check the Claims History.")
        else:
            st.error("Insufficient quantity or invalid ID.")

elif choice == "Claims History":
    st.title("📜 Transaction Log")
    claims_df = db.get_all_claims()
    st.table(claims_df)

elif choice == "Manage Claims":
    st.title("📋 Manage Claims Status")
    claims_df = db.get_all_claims()

    if claims_df.empty:
        st.info("No claims found yet.")
    else:
        st.dataframe(claims_df, use_container_width=True)

    st.subheader("Update Claim Status")
    with st.form("update_claim_form"):
        claim_id = st.number_input("Claim ID", min_value=1, step=1)
        new_status = st.selectbox(
            "New Status",
            ["Pending", "Completed", "Rejected", "Cancelled"],
        )
        if st.form_submit_button("Update Status"):
            updated = db.update_claim_status(claim_id, new_status)
            if updated:
                st.success(f"Claim {claim_id} status updated to {new_status}.")
                st.rerun()
            else:
                st.error("Claim ID not found. Please enter a valid Claim ID.")

elif choice == "SQL Insights":
    st.title("📈 SQL Insights Dashboard")
    st.write("Explore outputs of all 15 SQL queries with shared filters.")

    filter_options = db.get_filter_options()
    c1, c2, c3, c4 = st.columns(4)
    selected_city = c1.selectbox("City", ["All"] + filter_options['cities'], key="sql_city")
    selected_provider = c2.selectbox("Provider", ["All"] + filter_options['providers'], key="sql_provider")
    selected_food_type = c3.selectbox("Food Type", ["All"] + filter_options['food_types'], key="sql_food_type")
    selected_meal_type = c4.selectbox("Meal Type", ["All"] + filter_options['meal_types'], key="sql_meal_type")

    selected_filters = {
        'city': selected_city,
        'provider': selected_provider,
        'food_type': selected_food_type,
        'meal_type': selected_meal_type,
    }

    query_outputs = db.run_analytics_queries(selected_filters)

    for result in query_outputs:
        st.subheader(result['title'])
        if result['data'].empty:
            st.info("No rows returned for the selected filters.")
        else:
            st.dataframe(result['data'], use_container_width=True)

    st.subheader("Provider Contacts For Direct Coordination")
    contacts_df = db.get_provider_contacts(selected_filters)
    if contacts_df.empty:
        st.info("No provider contacts found for selected filters.")
    else:
        st.dataframe(contacts_df, use_container_width=True)