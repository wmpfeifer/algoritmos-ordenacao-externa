import os
import tempfile
import heapq
from typing import List, Iterator, Optional, BinaryIO, TextIO
import pickle
import json


class ExternalBalancedMergeSort:
    def __init__(self, memory_limit: int = 1000, temp_dir: str = None):
        """
        Inicializa o algoritmo de intercalação balanceada para arquivos externos.

        """
        self.memory_limit = memory_limit
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.tape_counter = 0

        # Garante que o diretório temporário existe
        os.makedirs(self.temp_dir, exist_ok=True)

    def create_tape(self, prefix: str) -> str:
        """Cria um arquivo temporário para simular uma fita."""
        self.tape_counter += 1
        return os.path.join(self.temp_dir, f"{prefix}_{self.tape_counter}.tmp")

    def read_input_file(self, input_file: str) -> Iterator[int]:
        """
        Lê dados do arquivo de entrada, suportando diferentes formatos.

        """
        try:
            with open(input_file, 'r') as f:
                # Tenta primeiro como JSON
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        for item in data:
                            yield int(item)
                        return
                except json.JSONDecodeError:
                    pass

                # Se não for JSON, tenta como números separados por linha ou espaço
                f.seek(0)
                content = f.read().strip()

                # Tenta separar por linhas primeiro
                lines = content.split('\n')
                for line in lines:
                    line = line.strip()
                    if line:
                        # Se a linha tem múltiplos números, separa por espaços
                        numbers = line.split()
                        for num_str in numbers:
                            try:
                                yield int(num_str)
                            except ValueError:
                                continue

        except FileNotFoundError:
            raise FileNotFoundError(f"Arquivo de entrada não encontrado: {input_file}")
        except Exception as e:
            raise ValueError(f"Erro ao ler arquivo de entrada: {e}")

    def write_output_file(self, output_file: str, sorted_data: List[int], format_type: str = 'json'):
        """
        Escreve os dados ordenados no arquivo de saída.

        """
        try:
            with open(output_file, 'w') as f:
                if format_type == 'json':
                    json.dump(sorted_data, f, indent=2)
                elif format_type == 'txt_lines':
                    for item in sorted_data:
                        f.write(f"{item}\n")
                elif format_type == 'txt_space':
                    f.write(' '.join(map(str, sorted_data)))
                else:
                    raise ValueError(f"Formato não suportado: {format_type}")

        except Exception as e:
            raise ValueError(f"Erro ao escrever arquivo de saída: {e}")

    def write_run_to_tape(self, data: List[int], tape_path: str, append: bool = False):
        """Escreve uma sequência ordenada (run) para uma fita."""
        mode = 'ab' if append else 'wb'
        with open(tape_path, mode) as f:
            # Escreve o tamanho da sequência seguido dos dados
            pickle.dump(len(data), f)
            for item in data:
                pickle.dump(item, f)

    def read_run_from_tape(self, tape_file: BinaryIO) -> Optional[List[int]]:
        """Lê uma sequência ordenada de uma fita."""
        try:
            run_length = pickle.load(tape_file)
            run = []
            for _ in range(run_length):
                run.append(pickle.load(tape_file))
            return run
        except EOFError:
            return None

    def create_initial_runs_from_file(self, input_file: str) -> List[str]:
        """
        Fase 1: Cria as sequências iniciais ordenadas lendo diretamente do arquivo.
        Lê o arquivo em blocos que cabem na memória, ordena cada bloco,
        e distribui alternadamente entre duas fitas.
        """
        tape_a = self.create_tape("initial_a")
        tape_b = self.create_tape("initial_b")

        runs_count = 0
        current_tape = 0  # 0 para tape_a, 1 para tape_b
        buffer = []

        print(f"Lendo arquivo de entrada: {input_file}")

        # Lê o arquivo em blocos
        for number in self.read_input_file(input_file):
            buffer.append(number)

            # Quando o buffer atinge o limite de memória, processa
            if len(buffer) >= self.memory_limit:
                buffer.sort()  # Ordena o bloco na memória

                # Distribui alternadamente entre as fitas
                target_tape = tape_a if current_tape == 0 else tape_b
                append_mode = runs_count > 0 and current_tape == (runs_count % 2)
                self.write_run_to_tape(buffer, target_tape, append=append_mode)

                current_tape = 1 - current_tape  # Alterna entre as fitas
                runs_count += 1
                buffer = []  # Limpa o buffer

                if runs_count % 100 == 0:
                    print(f"Processadas {runs_count} sequências...")

        # Processa o último bloco se houver dados restantes
        if buffer:
            buffer.sort()
            target_tape = tape_a if current_tape == 0 else tape_b
            append_mode = runs_count > 0 and current_tape == (runs_count % 2)
            self.write_run_to_tape(buffer, target_tape, append=append_mode)
            runs_count += 1

        print(f"Criadas {runs_count} sequências iniciais")
        return [tape_a, tape_b]

    def merge_runs(self, run1: List[int], run2: List[int]) -> List[int]:
        """Intercala duas sequências ordenadas."""
        result = []
        i = j = 0

        while i < len(run1) and j < len(run2):
            if run1[i] <= run2[j]:
                result.append(run1[i])
                i += 1
            else:
                result.append(run2[j])
                j += 1

        # Adiciona os elementos restantes
        result.extend(run1[i:])
        result.extend(run2[j:])

        return result

    def merge_phase(self, input_tapes: List[str], output_tapes: List[str]) -> int:
        """
        Fase de intercalação: lê sequências das fitas de entrada,
        intercala-as, e escreve nas fitas de saída.

        Returns:
            Número de sequências criadas nesta fase
        """
        # Abre as fitas de entrada
        tape_files = [open(tape, 'rb') for tape in input_tapes]

        sequences_created = 0
        current_output = 0

        try:
            while True:
                # Lê uma sequência de cada fita de entrada
                runs = []
                for tape_file in tape_files:
                    run = self.read_run_from_tape(tape_file)
                    if run is not None:
                        runs.append(run)

                # Se não há mais sequências para processar, termina
                if not runs:
                    break

                # Intercala as sequências disponíveis
                if len(runs) == 1:
                    merged_run = runs[0]
                else:
                    merged_run = self.merge_runs(runs[0], runs[1])

                # Escreve a sequência intercalada na fita de saída
                output_tape = output_tapes[current_output]
                append_mode = sequences_created > 0 and current_output == (sequences_created % 2)
                self.write_run_to_tape(merged_run, output_tape, append=append_mode)

                current_output = 1 - current_output  # Alterna entre as fitas de saída
                sequences_created += 1

        finally:
            # Fecha todas as fitas de entrada
            for tape_file in tape_files:
                tape_file.close()

        return sequences_created

    def sort_file(self, input_file: str, output_file: str, output_format: str = 'json') -> bool:
        """
        Executa o algoritmo completo de intercalação balanceada em arquivos.

        Args:
            input_file: Caminho para o arquivo de entrada
            output_file: Caminho para o arquivo de saída
            output_format: Formato do arquivo de saída ('json', 'txt_lines', 'txt_space')

        Returns:
            True se a ordenação foi bem-sucedida, False caso contrário
        """
        try:
            print(f"Iniciando ordenação externa do arquivo: {input_file}")
            print(f"Limite de memória: {self.memory_limit} elementos")
            print(f"Arquivo de saída: {output_file}")

            # Fase 1: Criar sequências iniciais a partir do arquivo
            current_tapes = self.create_initial_runs_from_file(input_file)

            # Fase 2: Intercalação iterativa
            pass_number = 1

            while True:
                # Cria novas fitas de saída
                output_tapes = [self.create_tape(f"pass_{pass_number}_a"),
                                self.create_tape(f"pass_{pass_number}_b")]

                # Executa uma passada de intercalação
                sequences_created = self.merge_phase(current_tapes, output_tapes)

                print(f"Passada {pass_number}: {sequences_created} sequências criadas")

                # Remove as fitas antigas
                for tape in current_tapes:
                    if os.path.exists(tape):
                        os.remove(tape)

                # Se criou apenas uma sequência, a ordenação está completa
                if sequences_created == 1:
                    break

                # Prepara para a próxima passada
                current_tapes = output_tapes
                pass_number += 1

            # Lê o resultado final e escreve no arquivo de saída
            final_tape = output_tapes[0]
            with open(final_tape, 'rb') as f:
                sorted_data = self.read_run_from_tape(f)

            if sorted_data:
                self.write_output_file(output_file, sorted_data, output_format)
                print(f"Ordenação concluída! Resultado salvo em: {output_file}")
                print(f"Total de elementos ordenados: {len(sorted_data)}")
                return True
            else:
                print("Erro: Nenhum dado foi processado")
                return False

        except Exception as e:
            print(f"Erro durante a ordenação: {e}")
            return False

        finally:
            # Limpa arquivos temporários
            self.cleanup()

    def cleanup(self):
        """Remove todos os arquivos temporários."""
        try:
            for filename in os.listdir(self.temp_dir):
                file_path = os.path.join(self.temp_dir, filename)
                if os.path.isfile(file_path):
                    os.remove(file_path)
            # Remove o diretório se estiver vazio
            if not os.listdir(self.temp_dir):
                os.rmdir(self.temp_dir)
        except OSError as e:
            print(f"Aviso: Não foi possível limpar todos os arquivos temporários: {e}")


# Funções auxiliares para demonstração e teste
def create_test_input_file(filename: str, size: int = 10000, format_type: str = 'json'):
    """Cria um arquivo de teste com dados aleatórios."""
    import random

    data = [random.randint(1, 100000) for _ in range(size)]

    with open(filename, 'w') as f:
        if format_type == 'json':
            json.dump(data, f)
        elif format_type == 'txt_lines':
            for item in data:
                f.write(f"{item}\n")
        elif format_type == 'txt_space':
            f.write(' '.join(map(str, data)))

    print(f"Arquivo de teste criado: {filename} com {size} elementos")
    return data


def verify_sorted_file(filename: str, original_data: List[int] = None) -> bool:
    """Verifica se o arquivo de saída está ordenado corretamente."""
    try:
        with open(filename, 'r') as f:
            sorted_data = json.load(f)

        # Verifica se está ordenado
        is_sorted = all(sorted_data[i] <= sorted_data[i + 1] for i in range(len(sorted_data) - 1))

        # Verifica se contém os mesmos elementos (se dados originais fornecidos)
        if original_data:
            same_elements = sorted(original_data) == sorted_data
            return is_sorted and same_elements

        return is_sorted

    except Exception as e:
        print(f"Erro ao verificar arquivo: {e}")
        return False


def demonstrate_external_sort():
    """Demonstra o uso do algoritmo de intercalação balanceada com arquivos."""

    # Configuração dos arquivos
    input_file = "input_data.json"
    output_file = "sorted_data.json"

    try:
        # Cria arquivo de teste
        print("=== Criando arquivo de teste ===")
        original_data = create_test_input_file(input_file, size=5000, format_type='json')

        # Executa a ordenação externa
        print("\n=== Executando ordenação externa ===")
        sorter = ExternalBalancedMergeSort(memory_limit=500)  # Limita memória para forçar uso externo

        success = sorter.sort_file(input_file, output_file, output_format='json')

        if success:
            # Verifica o resultado
            print("\n=== Verificando resultado ===")
            is_correct = verify_sorted_file(output_file, original_data)
            print(f"Resultado correto: {is_correct}")

            # Mostra estatísticas
            with open(output_file, 'r') as f:
                sorted_data = json.load(f)

            print(f"Elementos ordenados: {len(sorted_data)}")
            print(f"Menor elemento: {sorted_data[0]}")
            print(f"Maior elemento: {sorted_data[-1]}")
            print(f"Primeiros 10: {sorted_data[:10]}")
            print(f"Últimos 10: {sorted_data[-10:]}")

        # Teste com diferentes formatos
        print("\n=== Teste com formato de texto ===")
        input_txt = "input_data.txt"
        output_txt = "sorted_data.txt"

        create_test_input_file(input_txt, size=1000, format_type='txt_lines')
        sorter_txt = ExternalBalancedMergeSort(memory_limit=100)

        success_txt = sorter_txt.sort_file(input_txt, output_txt, output_format='txt_lines')

        if success_txt:
            print(f"Ordenação de arquivo texto concluída: {output_txt}")

    except Exception as e:
        print(f"Erro na demonstração: {e}")



# Executa a demonstração
if __name__ == "__main__":
    demonstrate_external_sort()