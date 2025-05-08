import concurrent.futures

def parallel_reduce(data, func, identity):
    while len(data) > 1:
        result = []
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Chunk in pairs
            futures = []
            for i in range(0, len(data), 2):
                a = data[i]
                b = data[i+1] if i+1 < len(data) else identity
                futures.append(executor.submit(func, a, b))
            result = [f.result() for f in futures]
        data = result
    return data[0]

def main():
    data = [4, 7, 2, 9, 1, 5, 8, 6]

    total_sum = parallel_reduce(data, lambda x, y: x + y, 0)
    minimum = parallel_reduce(data, min, float('inf'))
    maximum = parallel_reduce(data, max, float('-inf'))
    average = total_sum / len(data)

    print("Data:", data)
    print("Sum:", total_sum)
    print("Min:", minimum)
    print("Max:", maximum)
    print("Average:", average)

if __name__ == "__main__":
    main()
