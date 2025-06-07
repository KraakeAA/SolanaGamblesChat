// lib/litecoin-payment.js

import bip39 from 'bip39';
import * as bip32 from 'bip32';
import * as bitcoin from 'bitcoinjs-lib';
import axios from 'axios';

// --- Litecoin Network Configuration ---
const LITECOIN_NETWORK = {
    messagePrefix: '\x19Litecoin Signed Message:\n',
    bech32: 'ltc',
    bip32: {
        public: 0x019da462,
        private: 0x019d9cfe,
    },
    pubKeyHash: 0x30,
    scriptHash: 0x32, // For P2SH addresses
    wif: 0xb0,
};

const LITECOIN_API_TOKEN = process.env.BLOCKCYPHER_API_TOKEN;
const LITECOIN_API_BASE_URL = `https://api.blockcypher.com/v1/ltc/main`;
const FEE_PER_BYTE_LITOSHIS = 2; // A conservative fee estimate (in litoshis per byte)

/**
 * Derives a Litecoin P2WPKH (Bech32) address from a master seed and derivation path.
 * @param {string} seedPhrase - The BIP39 mnemonic.
 * @param {string} derivationPath - The BIP44 derivation path (e.g., m/44'/2'/0'/0/0).
 * @returns {{ address: string, keyPair: ECPairInterface }} The derived address and keypair object.
 */
export function deriveLitecoinAddress(seedPhrase, derivationPath) {
    const seed = bip39.mnemonicToSeedSync(seedPhrase);
    const root = bip32.fromSeed(seed, LITECOIN_NETWORK);
    const keyPair = root.derivePath(derivationPath);
    const { address } = bitcoin.payments.p2wpkh({ pubkey: keyPair.publicKey, network: LITECOIN_NETWORK });
    return { address, keyPair };
}

/**
 * Fetches the balance and unspent transaction outputs (UTXOs) for a Litecoin address.
 * @param {string} address - The Litecoin address to query.
 * @returns {Promise<{balance: bigint, utxos: any[]}>}
 */
export async function getLtcBalanceAndUtxos(address) {
    try {
        const url = `${LITECOIN_API_BASE_URL}/addrs/${address}?unspentOnly=true&includeScript=true`;
        const response = await axios.get(url, { params: { token: LITECOIN_API_TOKEN } });
        const balance = BigInt(response.data.final_balance || 0);
        const utxos = response.data.txrefs || [];
        return { balance, utxos };
    } catch (error) {
        console.error(`[LTC_API] Error fetching balance/UTXOs for ${address}: ${error.message}`);
        // If address is not found (404), it's not an error, just means it has no history.
        if (error.response && error.response.status === 404) {
            return { balance: 0n, utxos: [] };
        }
        throw error; // Re-throw other errors
    }
}

/**
 * Builds, signs, and broadcasts a transaction to sweep all funds from a deposit address.
 * @param {ECPairInterface} keyPair - The keypair of the deposit address.
 * @param {string} targetAddress - The central hot wallet address to send funds to.
 * @returns {Promise<string>} The transaction hash (txid).
 */
export async function sweepLitecoinAddress(keyPair, targetAddress) {
    const { address } = bitcoin.payments.p2wpkh({ pubkey: keyPair.publicKey, network: LITECOIN_NETWORK });
    const { balance, utxos } = await getLtcBalanceAndUtxos(address);

    if (utxos.length === 0) {
        throw new Error("No UTXOs to sweep.");
    }

    const psbt = new bitcoin.Psbt({ network: LITECOIN_NETWORK });
    let totalInput = 0;

    for (const utxo of utxos) {
        totalInput += utxo.value;
        const txDetails = await axios.get(`${LITECOIN_API_BASE_URL}/txs/${utxo.tx_hash}?includeHex=true`, { params: { token: LITECOIN_API_TOKEN }});
        psbt.addInput({
            hash: utxo.tx_hash,
            index: utxo.tx_output_n,
            witnessUtxo: {
                script: Buffer.from(txDetails.data.outputs[utxo.tx_output_n].script, 'hex'),
                value: utxo.value,
            },
        });
    }

    const transactionSize = psbt.toBuffer().length + (psbt.inputCount * 70); // Rough estimation
    const estimatedFee = transactionSize * FEE_PER_BYTE_LITOSHIS;
    const amountToSend = totalInput - estimatedFee;

    if (amountToSend <= 0) {
        throw new Error("Balance is too low to cover transaction fees.");
    }

    psbt.addOutput({
        address: targetAddress,
        value: amountToSend,
    });

    for (let i = 0; i < psbt.inputCount; i++) {
        psbt.signInput(i, keyPair);
    }
    
    psbt.validateSignaturesOfInput(0);
    psbt.finalizeAllInputs();

    const txHex = psbt.extractTransaction().toHex();

    // Broadcast the transaction
    const broadcastUrl = `${LITECOIN_API_BASE_URL}/txs/push`;
    const response = await axios.post(broadcastUrl, { tx: txHex }, { params: { token: LITECOIN_API_TOKEN } });
    
    return response.data.tx.hash;
}
