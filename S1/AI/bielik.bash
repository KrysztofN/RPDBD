# 0. Sprawdzenie karty 
# jeśli nie działa: sudo ubuntu-drivers install && sudo reboot
nvidia-smi

# 1. Instalacja potrzebnych narzędzi (+ CUDA)
sudo apt install -y git cmake build-essential libcurl4-openssl-dev tmux nvidia-cuda-toolkit   # GPU

# 2. Pobranie i zbudowanie llama.cpp z obsługą GPU
git clone https://github.com/ggml-org/llama.cpp
cd llama.cpp
cmake -B build -DGGML_CUDA=ON && cmake --build build -j            

# 3. Start w tmux
# ctrl + B -> D odłączenie od sesji
# tmux attach -t bielik przyłączenie do sesji
tmux new -s bielik
./build/bin/llama-server -hf speakleash/Bielik-11B-v3.0-Instruct-GGUF:Q8_0 \
  --port 8080 -ngl 99 -c 16384 --temp 0.3                           

# 4. Tunelowanie portu 
ssh -N -L 8080:localhost:8080 uzytkownik@adres-serwera