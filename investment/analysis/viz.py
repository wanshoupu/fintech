from matplotlib import pyplot as plt


def na_viz(hist):
    plt.figure(figsize=(10, 6))
    plt.bar(range(len(hist)), hist, color='skyblue')
    plt.xlabel('Number of Top NA Columns Dropped')
    plt.ylabel('Remaining Rows After dropna()')
    plt.title('Effect of Dropping High-NA Columns on Row Retention')
    plt.xticks(range(len(hist)))  # show all ticks
    plt.grid(axis='y', linestyle='--', alpha=0.7)
    plt.tight_layout()
    plt.show()
