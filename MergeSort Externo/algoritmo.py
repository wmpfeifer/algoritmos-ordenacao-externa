import os
import heapq
import tempfile
import random

class ExternalMergeSort:
    def __init__(self, chunk_size=1000):
        """
        Inicializa o algoritmo de merge sort externo.
        """
        self.chunk_size = chunk_size
        self.temp_files = []

    def create_sample_file(self, filename, num_elements=10000):
        """
        Cria um arquivo de exemplo com números aleatórios para testar o algoritmo.
        """
        print(f"Criando arquivo de exemplo '{filename}' com {num_elements} números...")

        with open(filename, 'w') as f:
            for i in range(num_elements):
                f.write(f"{random.randint(1, 100000)}\n")

        print(f"Arquivo criado com sucesso!")

    def split_and_sort_chunks(self, input_file):
        """
        Divide o arquivo de entrada em pedaços menores e ordena cada um.
        """
        print(f"\n=== FASE 1: Dividindo arquivo em pedaços de {self.chunk_size} elementos ===")

        chunk_files = []
        chunk_number = 0

        with open(input_file, 'r') as f:
            while True:
                # Lê um pedaço do arquivo
                chunk = []
                for _ in range(self.chunk_size):
                    line = f.readline()
                    if not line:
                        break
                    chunk.append(int(line.strip()))

                if not chunk:
                    break

                # Ordena o pedaço na memória
                chunk.sort()

                # Salva o pedaço ordenado em um arquivo temporário
                temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False,
                                                        prefix=f'chunk_{chunk_number}_')
                chunk_files.append(temp_file.name)

                for num in chunk:
                    temp_file.write(f"{num}\n")
                temp_file.close()

                print(f"Pedaço {chunk_number}: {len(chunk)} elementos ordenados -> {temp_file.name}")
                chunk_number += 1

        print(f"Total de pedaços criados: {len(chunk_files)}")
        return chunk_files

    def merge_chunks(self, chunk_files, output_file):
        """
        Faz merge de todos os pedaços ordenados em um único arquivo final.
        """
        print(f"\n=== FASE 2: Fazendo merge de {len(chunk_files)} pedaços ===")

        # Abre todos os arquivos de pedaços
        file_handles = []
        for chunk_file in chunk_files:
            file_handles.append(open(chunk_file, 'r'))

        # Inicializa o heap com o primeiro elemento de cada arquivo
        heap = []
        for i, file_handle in enumerate(file_handles):
            line = file_handle.readline()
            if line:
                heapq.heappush(heap, (int(line.strip()), i))

        # Merge usando heap (priority queue)
        with open(output_file, 'w') as output:
            merge_count = 0

            while heap:
                # Pega o menor elemento do heap
                min_val, file_index = heapq.heappop(heap)
                output.write(f"{min_val}\n")
                merge_count += 1

                if merge_count % 1000 == 0:
                    print(f"Elementos processados no merge: {merge_count}")

                # Lê o próximo elemento do mesmo arquivo
                next_line = file_handles[file_index].readline()
                if next_line:
                    heapq.heappush(heap, (int(next_line.strip()), file_index))

        # Fecha todos os arquivos
        for file_handle in file_handles:
            file_handle.close()

        print(f"Merge concluído! Total de elementos: {merge_count}")

    def cleanup_temp_files(self, chunk_files):
        """
        Remove os arquivos temporários criados durante o processo.
        """
        print(f"\n=== LIMPEZA: Removendo {len(chunk_files)} arquivos temporários ===")

        for chunk_file in chunk_files:
            try:
                os.remove(chunk_file)
                print(f"Removido: {chunk_file}")
            except OSError as e:
                print(f"Erro ao remover {chunk_file}: {e}")

    def external_merge_sort(self, input_file, output_file):
        """
        Executa o algoritmo completo de merge sort externo.
        """
        print("=" * 60)
        print("INICIANDO MERGE SORT EXTERNO")
        print("=" * 60)

        # Verifica se o arquivo de entrada existe
        if not os.path.exists(input_file):
            print(f"Erro: Arquivo '{input_file}' não encontrado!")
            return

        try:
            # Fase 1: Dividir e ordenar pedaços
            chunk_files = self.split_and_sort_chunks(input_file)

            # Fase 2: Fazer merge dos pedaços
            self.merge_chunks(chunk_files, output_file)

            # Limpeza
            self.cleanup_temp_files(chunk_files)

            print("\n" + "=" * 60)
            print("MERGE SORT EXTERNO CONCLUÍDO COM SUCESSO!")
            print("=" * 60)

        except Exception as e:
            print(f"Erro durante a execução: {e}")

    def verify_sorted(self, filename):
        """
        Verifica se um arquivo está ordenado corretamente.
        """
        print(f"\n=== VERIFICAÇÃO: Checando se '{filename}' está ordenado ===")

        try:
            with open(filename, 'r') as f:
                prev_num = float('-inf')
                count = 0

                for line in f:
                    current_num = int(line.strip())
                    count += 1

                    if current_num < prev_num:
                        print(f"❌ Arquivo NÃO está ordenado! Erro na linha {count}")
                        return False

                    prev_num = current_num

                print(f"✅ Arquivo está ORDENADO corretamente! Total: {count} elementos")
                return True

        except Exception as e:
            print(f"Erro ao verificar arquivo: {e}")
            return False


# Exemplo de uso
if __name__ == "__main__":
    # Configurações
    INPUT_FILE = "numeros_desordenados.txt"
    OUTPUT_FILE = "numeros_ordenados.txt"
    CHUNK_SIZE = 1000  # Ajuste conforme a memória disponível

    # Cria uma instância do algoritmo
    sorter = ExternalMergeSort(chunk_size=CHUNK_SIZE)

    # Cria um arquivo de exemplo para testar
    sorter.create_sample_file(INPUT_FILE, num_elements=5000)

    # Executa o merge sort externo
    sorter.external_merge_sort(INPUT_FILE, OUTPUT_FILE)

    # Verifica se o resultado está correto
    sorter.verify_sorted(OUTPUT_FILE)

    # Mostra uma amostra do resultado
    print(f"\n=== AMOSTRA DO RESULTADO ===")
    print("Primeiros 10 números ordenados:")
    try:
        with open(OUTPUT_FILE, 'r') as f:
            for i, line in enumerate(f):
                if i >= 10:
                    break
                print(f"{i + 1}: {line.strip()}")
    except Exception as e:
        print(f"Erro ao ler arquivo de saída: {e}")