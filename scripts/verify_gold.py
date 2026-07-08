#!/usr/bin/env python3
import sqlite3

def run_verification():
    conn = sqlite3.connect("data/warehouse.db")
    cur = conn.cursor()
    
    # 1. Check for orphaned rows
    cur.execute("SELECT COUNT(*) FROM fact_orders WHERE customer_sk NOT IN (SELECT customer_sk FROM dim_customers);")
    orphaned_count = cur.fetchone()[0]
    print(f"--- 1. Orphaned Rows Check ---")
    print(f"Number of orphaned rows in fact_orders: {orphaned_count}")
    assert orphaned_count == 0, "Error: Orphaned facts detected!"
    
    # 2. Check customer versions join mapping
    cur.execute("SELECT customer_id, COUNT(*) FROM dim_customers WHERE customer_id != 'unknown' GROUP BY customer_id HAVING COUNT(*) > 1 LIMIT 3;")
    multi_version_custs = cur.fetchall()
    
    print("\n--- 2. Historical Join Verification (SCD2 Mapping) ---")
    for cid, cnt in multi_version_custs:
        print(f"\nCustomer: {cid} (Versions: {cnt})")
        cur.execute("SELECT customer_sk, email, state, valid_from, valid_to FROM dim_customers WHERE customer_id = ? ORDER BY valid_from;", (cid,))
        dimensions = cur.fetchall()
        for idx, dim in enumerate(dimensions):
            print(f"  Version {idx + 1} Dim SK: {dim[0]} | Email: {dim[1]} | State: {dim[2]} | Valid: [{dim[3]} -> {dim[4]})")
            
        cur.execute(
            """
            SELECT order_id, customer_sk, order_amount, created_at 
            FROM fact_orders 
            WHERE customer_sk IN (SELECT customer_sk FROM dim_customers WHERE customer_id = ?) 
            ORDER BY created_at;
            """, 
            (cid,)
        )
        facts = cur.fetchall()
        print(f"  Associated Orders in fact_orders:")
        if not facts:
            print("    No orders found for this customer.")
        else:
            for f in facts:
                print(f"    Order ID: {f[0]} | Linked SK: {f[1]} | Amount: ${f[2]} | Placed: {f[3]}")
                
    # 3. Check for quarantined fallback keys (customer_sk = '-1')
    cur.execute("SELECT COUNT(*) FROM fact_orders WHERE customer_sk = '-1';")
    unknown_count = cur.fetchone()[0]
    print(f"\n--- 3. Default Fallback Check ---")
    print(f"Number of orders mapped to 'Unknown Customer' (customer_sk = -1): {unknown_count}")
    
    conn.close()

if __name__ == "__main__":
    run_verification()
