# visualize_results.py
import pandas as pd
import matplotlib.pyplot as plt

print("Creating visualizations from CSV files...")

try:
    # Load data
    top_products = pd.read_csv('top_products.csv')
    revenue_by_category = pd.read_csv('revenue_by_category.csv')
    
    # 1. Top Products Bar Chart
    plt.figure(figsize=(10, 6))
    plt.bar(top_products['product_id'].head(10), top_products['totalSold'].head(10))
    plt.title('Top 10 Products by Units Sold')
    plt.xlabel('Product ID')
    plt.ylabel('Units Sold')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('top_products_chart.png')
    print("Saved: top_products_chart.png")
    
    # 2. Revenue by Category Bar Chart
    plt.figure(figsize=(10, 6))
    plt.bar(revenue_by_category['category_id'], revenue_by_category['totalRevenue'])
    plt.title('Revenue by Category')
    plt.xlabel('Category ID')
    plt.ylabel('Revenue ($)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig('revenue_by_category_chart.png')
    print("Saved: revenue_by_category_chart.png")
    
    # 3. Show summary statistics
    print("\n=== SUMMARY STATISTICS ===")
    print(f"\nTop Products Analysis:")
    print(f"Total products analyzed: {len(top_products)}")
    print(f"Total units sold: {top_products['totalSold'].sum():,}")
    print(f"Total revenue: ${top_products['totalRevenue'].sum():,.2f}")
    
    print(f"\nCategory Analysis:")
    print(f"Total categories: {len(revenue_by_category)}")
    print(f"Total category revenue: ${revenue_by_category['totalRevenue'].sum():,.2f}")
    print(f"Average revenue per category: ${revenue_by_category['totalRevenue'].mean():,.2f}")
    
    plt.show()
    
except Exception as e:
    print(f"Error: {e}")
    print("Make sure CSV files exist in the current directory")
