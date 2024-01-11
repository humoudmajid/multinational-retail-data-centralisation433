class DataCleaning:

    def __init__(self):
        pass

    def clean_user_data(self, user_data):
        user_data.dropna(inplace=True) 
        return user_data

    def clean_single_table(self, df_table):
        # Cleaning operation on a single DataFrame
        cleaned_df = df_table.dropna()

        return cleaned_df

    def clean_card_data(self, card_data):
        cleaned_tables = []

        for df_table in card_data:
            # Clean each DataFrame in the list
            cleaned_df = self.clean_single_table(df_table)
            cleaned_tables.append(cleaned_df)

        return cleaned_tables

    def clean_store_data(self, store_data):
        store_data.dropna(inplace=True)
        return store_data

    def clean_products_data(self, products_data):
        products_data.dropna(inplace=True)
        return products_data

    def clean_orders_data(self, orders_data):
        columns_to_remove = ['first_name', 'last_name', '1', 'level_0']
        orders_data.drop(columns=columns_to_remove, errors='ignore', inplace=True)
        return orders_data

    def convert_product_weights(self, products_df):
            def convert_weight(weight_str):
                weight_str = weight_str.lower()
                if 'kg' in weight_str:
                    return float(weight_str.replace('kg', '').strip())
                if 'g' in weight_str:
                    return float(weight_str.replace('g', '').strip()) / 1000
                if 'ml' in weight_str:
                    return float(weight_str.replace('ml', '').strip()) / 1000
                return 0.0
            
            products_df['Weight'] = products_df['Weight'].apply(convert_weight)
            return products_df

